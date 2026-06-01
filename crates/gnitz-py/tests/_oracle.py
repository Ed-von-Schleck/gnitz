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
"""

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

    ``UNION ALL`` sums weights (the only multiplicity-preserving op). The
    deduplicating ops collapse to weight 1 per surviving tuple, where
    "surviving" is decided on *set membership* (net weight > 0) of each side:

      * ``UNION``     — tuple present in either side
      * ``INTERSECT`` — tuple present in both sides
      * ``EXCEPT``    — tuple present in the left but not the right
    """
    op = op.upper().strip()
    if op == "UNION ALL":
        return Counter(left) + Counter(right)
    lset = {k for k, w in left.items() if w > 0}
    rset = {k for k, w in right.items() if w > 0}
    if op == "UNION":
        keys = lset | rset
    elif op == "INTERSECT":
        keys = lset & rset
    elif op == "EXCEPT":
        keys = lset - rset
    else:
        raise ValueError(f"oracle_setop: unknown op {op!r}")
    return Counter({k: 1 for k in keys})


# ── view scan + comparison ─────────────────────────────────────────────────

def scan_multiset(client, view_tid, project):
    """Scan ``view_tid`` and return the weight-multiset over ``project`` columns.

    Weights are summed per projected tuple, so multiple physical rows that share
    projected content (e.g. UNION ALL's two branches, distinct synthetic PKs)
    coalesce to one entry with the combined weight. The synthetic PKs
    ``_join_pk`` / ``_set_pk`` are excluded automatically — ``project`` only
    lists meaningful columns, so they are never read. A negative *net* weight
    means a retraction underflowed and is a hard error.
    """
    c = Counter()
    for r in client.scan(view_tid):
        d = r._asdict()
        c[tuple(d[col] for col in project)] += r.weight
    for k, w in c.items():
        assert w >= 0, f"negative net weight {w} for tuple {k} in view {view_tid}"
    return Counter({k: w for k, w in c.items() if w != 0})


def assert_view_matches(client, view_tid, project, expected, ctx=""):
    """Assert the view's weight-multiset equals ``expected``.

    On mismatch, prints a per-tuple expected-vs-actual weight diff for every
    tuple that differs (missing, extra, or wrong weight).
    """
    expected = Counter({k: w for k, w in expected.items() if w != 0})
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
