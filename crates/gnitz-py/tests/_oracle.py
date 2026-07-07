"""Differential oracle harness for incremental-view correctness tests.

Underscore-prefixed so pytest does not collect it as a test module; the tests
directory is on ``sys.path`` so test files import it as ``import _oracle``.

The oracle is *pure Python ground truth* recomputed from base state that the
test itself maintains — never scanned back from the DB. Recomputing from an
independent source is the whole point: a base-table scan bug cannot mask a view
bug, because the expected value never touches the engine.

Comparison is by **weight-multiset**, not row-count. Each tuple's net weight is
the sum of ``row.weight`` over every physical view row that projects to it,
grouped by the meaningful (non-synthetic-PK) columns. Weight-sum — not a plain
row count — is required because:

  * ``UNION ALL`` of overlapping branches legitimately carries weight 2;
  * the same-source set-op bug manifests as a *wrong weight* (e.g. a row that
    should net to weight 0 surviving at weight 1), which a row-count check that
    ignores weight would miss;
  * a retraction bug shows up as a negative net weight, which the comparator
    asserts can never happen.

State representation
--------------------
A base relation's state is a ``dict`` mapping primary-key value -> row-dict
(column-name -> value). The test applies INSERT/DELETE/UPDATE to this dict in
lockstep with the SQL it sends to the engine, then asserts the view matches the
oracle recomputed from the dict.

Named weight-multisets (composing for nested views)
---------------------------------------------------
The aggregate / DISTINCT primitives operate on a **named weight-multiset**: a
``Counter`` keyed by value-tuples paired with an ordered ``cols`` name list (the
same ``project``/``out_cols`` style the comparison helpers use). They return the
same shape, so a view over a ``GROUP BY``/``DISTINCT`` view is modelled by
chaining one call's output into the next. Bootstrap the first multiset from base
state with ``oracle_filter_project`` (projecting the union of the group columns
and every aggregate-argument column), then feed it through
``oracle_groupby_aggregate`` / ``oracle_distinct``.

Float canonicalization
----------------------
A scanned (or expected) value that is a Python ``float`` is canonicalized to its
raw IEEE-754 bits (``struct.pack("<d", v)``) before it enters a ``Counter`` key,
by both ``scan_multiset`` and ``assert_view_matches``. Without it ``Counter``
equality would be wrong two ways: ``nan != nan`` (a bit-stable MIN/MAX NaN output
would never match itself) and ``-0.0 == 0.0`` (two distinct bit patterns would
merge). Integer/string/NULL values are left untouched, so an all-integer view
compares exactly as before. A **float-column** ``SUM``/``AVG`` is non-deterministic
between live tick-order and rebuild chunk-order (IEEE addition is non-associative)
and must still be omitted from ``project`` by the test author; canonicalization only
makes the *deterministic* float columns (``MIN``/``MAX`` copy verbatim bits, integer
``AVG`` is one exact division) compare bit-exactly.
"""

import struct
from collections import Counter


# ── base-state maintenance ────────────────────────────────────────────────

def apply_insert(state, pk_col, rows):
    """Insert ``rows`` (list of column-name -> value dicts) into ``state``.

    Every base row has weight +1 (base-table positivity). Re-inserting an
    existing PK is a test bug, so it is asserted against.
    """
    for row in rows:
        pk = row[pk_col]
        assert pk not in state, f"oracle: duplicate insert of pk {pk}"
        state[pk] = dict(row)


def apply_delete(state, pk_col, pks):
    """Delete the rows with the given primary keys from ``state``."""
    for pk in pks:
        state.pop(pk, None)


def apply_update(state, pk_col, pk, assignments):
    """Apply an UPDATE (column -> new value) to the row at ``pk``.

    UPDATE never touches the PK column (mirrors the engine's UPDATE = retract +
    re-insert at the same key); asserting that keeps the oracle honest.
    """
    assert pk_col not in assignments, "oracle: UPDATE must not touch the pk column"
    assert pk in state, f"oracle: UPDATE of absent pk {pk}"
    state[pk].update(assignments)


# ── per-branch ground truth (weight-multisets) ────────────────────────────

def oracle_filter_project(state, where, project):
    """Multiset of projected tuples for ``SELECT project FROM state [WHERE ...]``.

    ``where`` is a callable row-dict -> bool, or ``None`` for no predicate.
    ``project`` is the ordered list of column names to keep. Each surviving base
    row contributes weight +1 to its projected tuple.
    """
    c = Counter()
    for row in state.values():
        if where is None or where(row):
            c[tuple(row[col] for col in project)] += 1
    return c


def oracle_equijoin(left, lwhere, lkey, lproj,
                    right, rwhere, rkey, rproj, out_cols):
    """Multiset for an inner equijoin ``left ⋈ right`` on ``lkey == rkey``.

    Output tuples are ``(left lproj values..., right rproj values...)`` in the
    same order as ``out_cols`` (which must therefore be ``lproj + rproj``).
    NULL keys match nothing; a many-to-many key yields the full product.
    """
    assert len(out_cols) == len(lproj) + len(rproj), \
        "oracle_equijoin: out_cols must be lproj followed by rproj"
    lrows = [r for r in left.values() if lwhere is None or lwhere(r)]
    rrows = [r for r in right.values() if rwhere is None or rwhere(r)]
    c = Counter()
    for lr in lrows:
        lk = lr[lkey]
        if lk is None:           # NULL keys match nothing
            continue
        for rr in rrows:
            rk = rr[rkey]
            if rk is None:
                continue
            if lk == rk:
                vals = tuple(lr[col] for col in lproj) + tuple(rr[col] for col in rproj)
                c[vals] += 1
    return c


def oracle_setop(op, left, right):
    """Apply a set operation to two branch multisets.

    The **bag** (``ALL``) ops preserve per-tuple multiplicity (``cL``/``cR`` are
    the left/right counts):

      * ``UNION ALL``     — ``cL + cR``
      * ``EXCEPT ALL``    — ``max(0, cL − cR)``
      * ``INTERSECT ALL`` — ``min(cL, cR)``

    The deduplicating ops collapse to weight 1 per surviving tuple, where
    "surviving" is decided on *set membership* (net weight > 0) of each side:

      * ``UNION``     — tuple present in either side
      * ``INTERSECT`` — tuple present in both sides
      * ``EXCEPT``    — tuple present in the left but not the right
    """
    # The three bag ops are exactly Counter's native arithmetic, all of which
    # drop non-positive results (safe: base multiplicities are ≥ 0, so
    # `cL - cR` and `min(cL, cR)` are dropped only when they hit the `> 0`
    # boundary the bag semantics also clamp at).
    op = op.upper().strip()
    if op == "UNION ALL":
        return Counter(left) + Counter(right)       # cL + cR
    if op == "EXCEPT ALL":
        return Counter(left) - Counter(right)       # max(0, cL − cR)
    if op == "INTERSECT ALL":
        return Counter(left) & Counter(right)       # min(cL, cR)
    # The deduplicating ops decide membership exactly as DISTINCT does (net
    # weight > 0), so reuse that single source of truth for the boundary rule.
    lset = set(oracle_distinct(left))
    rset = set(oracle_distinct(right))
    if op == "UNION":
        keys = lset | rset
    elif op == "INTERSECT":
        keys = lset & rset
    elif op == "EXCEPT":
        keys = lset - rset
    else:
        raise ValueError(f"oracle_setop: unknown op {op!r}")
    return Counter({k: 1 for k in keys})


def oracle_distinct(rel):
    """Collapse a weight-multiset to weight 1 per tuple with net weight > 0.

    Models ``SELECT DISTINCT`` (and the deduplicating set-op core) per DBSP
    Proposition 4.7: a tuple is present at weight 1 iff its accumulated net
    weight is positive, independent of how many physical rows carry it. ``rel``
    is a ``Counter`` keyed by value-tuples; DISTINCT does not change the column
    list, so the caller keeps tracking it. Returns a new ``Counter``.
    """
    return Counter({k: 1 for k, w in rel.items() if w > 0})


def oracle_groupby_aggregate(rel, cols, group_names, aggs):
    """Weight-multiset for ``SELECT group_names, aggs FROM rel GROUP BY group_names``.

    ``rel``         — input weight-multiset: a ``Counter`` keyed by value-tuples
                      ordered per ``cols``; each tuple's weight is its row
                      multiplicity. Build the first one with
                      ``oracle_filter_project``, projecting the union of the group
                      columns and every aggregate-argument column.
    ``cols``        — ordered name list describing ``rel``'s tuples.
    ``group_names`` — subset of ``cols`` to group by.
    ``aggs``        — list of ``(out_name, kind, arg_col)`` specs; ``kind`` is one
                      of COUNT/SUM/MIN/MAX/AVG and ``arg_col`` is the source column
                      (``None`` only for COUNT(*)).

    Returns ``(weights, out_cols)`` — a named weight-multiset of the same shape as
    the input, so a nested view chains this output into another call. ``out_cols``
    is ``group_names`` followed by each agg's ``out_name``; every surviving group
    carries weight 1.

    Semantics mirror the engine exactly:
      * COUNT(*) sums row weight; COUNT(col)/SUM/MIN/MAX/AVG skip NULL inputs.
      * a group with no non-NULL input for an aggregate yields NULL there
        (SUM/AVG/MIN/MAX), while COUNT(*) still counts the rows.
      * integer SUM (and AVG's numerator) wrap mod 2**64 (engine ``wrapping_add``).
      * MIN/MAX select by IEEE total order (matching ``encode_ordered`` / the
        ``f64::total_cmp`` order) and emit the verbatim selected value.
      * AVG = SUM / COUNT(col) as a single ``f64`` division.
    """
    idx = {name: i for i, name in enumerate(cols)}
    arg_cols = {arg for (_o, _k, arg) in aggs if arg is not None}
    groups = {}   # group-tuple -> {"weight": int, "vals": {arg_col: [(value, weight)]}}
    for tup, w in rel.items():
        if w <= 0:
            continue
        gkey = tuple(tup[idx[g]] for g in group_names)
        slot = groups.get(gkey)
        if slot is None:
            slot = groups[gkey] = {"weight": 0, "vals": {a: [] for a in arg_cols}}
        slot["weight"] += w
        for a in arg_cols:
            slot["vals"][a].append((tup[idx[a]], w))
    # A no-GROUP-BY (ungrouped/global) aggregate is one logical group that exists
    # even over empty input: SQL scalar-aggregate semantics require exactly one row
    # (COUNT(*)=0, COUNT(col)=0, SUM/MIN/MAX/AVG=NULL via _agg_value's empty-group
    # arms). A plain GROUP BY emits a row only per non-empty input group, so seed
    # the lone empty group `()` when the grouping set is empty and no input row
    # formed one. Over a non-empty source the `()` group already exists.
    if not group_names and not groups:
        groups[()] = {"weight": 0, "vals": {a: [] for a in arg_cols}}
    out_cols = list(group_names) + [out for (out, _k, _a) in aggs]
    weights = Counter()
    for gkey, slot in groups.items():
        row = list(gkey) + [_agg_value(kind, arg, slot) for (_o, kind, arg) in aggs]
        weights[tuple(row)] += 1
    return weights, out_cols


def _agg_value(kind, arg, slot):
    """Compute one aggregate over a group ``slot`` (see ``oracle_groupby_aggregate``)."""
    if kind == "COUNT" and arg is None:
        return slot["weight"]                                  # COUNT(*): row weight
    nonnull = [(v, w) for (v, w) in slot["vals"][arg] if v is not None]
    if kind == "COUNT":
        return sum(w for (_v, w) in nonnull)                   # COUNT(col): non-null weight
    if not nonnull:
        return None                                            # empty / all-NULL group
    if kind == "SUM":
        return _wrap_i64(sum(v * w for (v, w) in nonnull))
    if kind == "AVG":
        total = _wrap_i64(sum(v * w for (v, w) in nonnull))
        count = sum(w for (_v, w) in nonnull)
        return float(total) / float(count)
    if kind in ("MIN", "MAX"):
        vals = [v for (v, _w) in nonnull]
        return (min if kind == "MIN" else max)(vals, key=_total_order_key)
    raise ValueError(f"oracle_groupby_aggregate: unknown agg kind {kind!r}")


def _wrap_i64(x):
    """Reduce a Python int to the signed 64-bit value the engine's ``wrapping_add``
    accumulator holds — mod 2**64, two's-complement — so an integer SUM matches the
    engine even on overflow."""
    return ((x + (1 << 63)) % (1 << 64)) - (1 << 63)


def _total_order_key(v):
    """MIN/MAX selection order matching the engine. Floats use the IEEE-754 total
    order (``f64::total_cmp`` / the order-preserving ``encode_ordered`` bit
    transform), so -0.0 < +0.0 and NaN sorts at the extremes exactly as the engine
    selects; every other type uses natural Python order."""
    if isinstance(v, float):
        b = struct.unpack("<Q", struct.pack("<d", v))[0]
        return b ^ (0xFFFFFFFFFFFFFFFF if b >> 63 else 0x8000000000000000)
    return v


# ── view scan + comparison ─────────────────────────────────────────────────

def _canon(v):
    """Canonicalize a scanned/expected value for use as a ``Counter`` key.

    A Python ``float`` becomes its raw IEEE-754 bits so bit-stable MIN/MAX (and
    integer-AVG) outputs compare exactly: NaN then compares equal to itself and
    -0.0 stays distinct from +0.0, which ``==`` would otherwise get wrong. All
    other types (int/str/bytes/None) pass through unchanged.
    """
    return struct.pack("<d", v) if isinstance(v, float) else v


def scan_multiset(client, view_tid, project):
    """Scan ``view_tid`` and return the weight-multiset over ``project`` columns.

    Weights are summed per projected tuple, so multiple physical rows that share
    projected content (e.g. UNION ALL's two branches, distinct synthetic PKs)
    coalesce to one entry with the combined weight. Synthetic PKs such as
    ``_join_pk`` / ``_set_pk`` are hidden columns, so ``r._asdict()`` omits them
    automatically and ``project`` only names visible, meaningful columns. Float
    columns are canonicalized to bits (``_canon``) so MIN/MAX/AVG values compare
    bit-exactly.
    A negative *net* weight means a retraction underflowed and is a hard error.
    """
    c = Counter()
    for r in client.scan(view_tid):
        d = r._asdict()
        c[tuple(_canon(d[col]) for col in project)] += r.weight
    for k, w in c.items():
        assert w >= 0, f"negative net weight {w} for tuple {k} in view {view_tid}"
    return Counter({k: w for k, w in c.items() if w != 0})


def assert_view_matches(client, view_tid, project, expected, ctx=""):
    """Assert the view's weight-multiset equals ``expected``.

    On mismatch, prints a per-tuple expected-vs-actual weight diff for every
    tuple that differs (missing, extra, or wrong weight). Expected tuples are
    float-canonicalized identically to the scan so MIN/MAX/AVG values match.
    """
    expected = Counter({tuple(_canon(v) for v in k): w
                        for k, w in expected.items() if w != 0})
    actual = scan_multiset(client, view_tid, project)
    if actual == expected:
        return
    all_keys = set(actual) | set(expected)
    lines = []
    for k in sorted(all_keys, key=repr):
        a, e = actual.get(k, 0), expected.get(k, 0)
        if a != e:
            lines.append(f"    {k}: expected weight {e}, got {a}")
    label = f" [{ctx}]" if ctx else ""
    raise AssertionError(
        f"view multiset mismatch{label} (project={project}):\n" + "\n".join(lines)
    )
