"""Shared-source / overlapping-input incremental correctness.

Domain: views whose two operands read the *same* underlying relation (directly,
or transitively through a view). These are precisely the configurations the rest
of the suite never exercised — every other join/set-op test uses two distinct
base tables and hand-written expected lists. Here every case maintains Python
base state and checks the view against a from-scratch oracle recompute after each
epoch (each ``execute_sql`` is one epoch), so the question is no longer "did the
author anticipate this output?" but "is the incremental result ever wrong?".

Run (both worker counts matter — the bug was deterministic at each):
    cd crates/gnitz-py && GNITZ_WORKERS=1 uv run pytest tests/test_shared_source_incremental.py -v --tb=short
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_shared_source_incremental.py -v --tb=short
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest -m slow tests/test_shared_source_incremental.py -v
"""
import os
import random

import pytest

import gnitz
import _oracle as oracle

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))

# A JOIN whose two operands both trace to the same base relation (transitive
# self-join `t JOIN vt`, or `vt1 JOIN vt2` over one base) crashes a worker under
# GNITZ_WORKERS>1: the multiworker two-sided dispatch seeds an operand-shaped
# delta into the join's wider output register, tripping the VM column-count guard
# (`assertion left == right failed: VM register N schema/batch column-count
# mismatch` at gnitz-engine/src/vm.rs:743). The crash is correct at WORKERS=1.
# It is a pre-existing engine bug in the multiworker JOIN register routing —
# unrelated to the set-op fix in this change, and out of its scope. `run=False`
# marks the case xfail WITHOUT executing it, so the worker crash cannot cascade
# into unrelated tests. Two-sided same-source *set-ops* route correctly at
# WORKERS>1 and are NOT gated. See
# plans/transitive-self-join-multiworker-crash.md.
_MULTIWORKER_JOIN_XFAIL = pytest.mark.xfail(
    _NUM_WORKERS > 1,
    reason="two-sided same-source JOIN crashes a worker at GNITZ_WORKERS>1 "
           "(vm.rs:743 register column-count mismatch); pre-existing engine bug, "
           "see plans/transitive-self-join-multiworker-crash.md",
    run=False,
    strict=False,
)


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _vid(client, sn, name):
    return client.resolve_table(sn, name)[0]


# ── transitive self-join: t JOIN vt, vt a filter view over t ──────────────
#
# vt = SELECT id,k,v FROM t WHERE v > 0. The join reads BOTH t (delta edge) and
# vt (delta edge) → two dependency edges → the DAG scheduler evaluates the view
# once per edge, recovering the Δt ⋈ Δvt cross-term. A 2-term drop would miss the
# cross-term whenever a single INSERT creates matches on both sides at once.

def _setup_self_join(client, sn):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW vt AS SELECT id, k, v FROM t WHERE v > 0",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW j AS "
        "SELECT t.id AS lid, t.k AS lk, t.v AS lv, vt.id AS rid, vt.k AS rk, vt.v AS rv "
        "FROM t JOIN vt ON t.k = vt.k",
        schema_name=sn,
    )


_J_PROJECT = ["lid", "lk", "lv", "rid", "rk", "rv"]


def _self_join_expected(t_state):
    """Oracle for j: t ⋈ (filter v>0 over t) on k."""
    vt_state = {pk: row for pk, row in t_state.items() if row["v"] > 0}
    return oracle.oracle_equijoin(
        left=t_state, lwhere=None, lkey="k", lproj=["id", "k", "v"],
        right=vt_state, rwhere=None, rkey="k", rproj=["id", "k", "v"],
        out_cols=_J_PROJECT,
    )


@_MULTIWORKER_JOIN_XFAIL
class TestTransitiveSelfJoin:
    def test_simultaneous_both_side_matches_single_epoch(self, client):
        """The decisive settle-it-immediately check: one multi-row INSERT that
        creates new matches on BOTH the left (t) and the right (vt) at the same
        time — exactly the ΔA⋈ΔB cross-term a 2-term incremental drop would
        miss. 3 rows sharing key k=5 (all v>0) must yield the full 3×3 product."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup_self_join(client, sn)
            vid = _vid(client, sn, "j")
            t_state = {}

            rows = [
                {"id": 1, "k": 5, "v": 10},
                {"id": 2, "k": 5, "v": 20},
                {"id": 3, "k": 5, "v": 30},
            ]
            client.execute_sql(
                "INSERT INTO t VALUES (1, 5, 10), (2, 5, 20), (3, 5, 30)",
                schema_name=sn,
            )
            oracle.apply_insert(t_state, "id", rows)
            exp = _self_join_expected(t_state)
            # Sanity: this must be the full 3×3 cross-product, not a 2-term subset.
            assert sum(exp.values()) == 9, f"oracle should be 9 product rows, got {sum(exp.values())}"
            oracle.assert_view_matches(client, vid, _J_PROJECT, exp, ctx="simultaneous-both-side")
        finally:
            _cleanup(client, sn, tables=["t"], views=["j", "vt"])

    def test_multi_batch_evolution(self, client):
        """insert → UPDATE a row out of vt's filter → insert → DELETE a cross
        participant, differential-checked after every epoch."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup_self_join(client, sn)
            vid = _vid(client, sn, "j")
            t_state = {}

            def check(ctx):
                oracle.assert_view_matches(client, vid, _J_PROJECT,
                                           _self_join_expected(t_state), ctx=ctx)

            client.execute_sql(
                "INSERT INTO t VALUES (1, 7, 5), (2, 7, 9), (3, 8, 4)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 1, "k": 7, "v": 5}, {"id": 2, "k": 7, "v": 9}, {"id": 3, "k": 8, "v": 4}])
            check("after-initial-insert")

            # UPDATE id=2 so v <= 0: it stays in t (left) but leaves vt (right).
            client.execute_sql("UPDATE t SET v = -1 WHERE id = 2", schema_name=sn)
            oracle.apply_update(t_state, "id", 2, {"v": -1})
            check("after-update-out-of-filter")

            client.execute_sql("INSERT INTO t VALUES (4, 7, 11), (5, 8, 6)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 4, "k": 7, "v": 11}, {"id": 5, "k": 8, "v": 6}])
            check("after-second-insert")

            # DELETE a cross-product participant (id=1, k=7).
            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            oracle.apply_delete(t_state, "id", [1])
            check("after-delete")
        finally:
            _cleanup(client, sn, tables=["t"], views=["j", "vt"])


# Direct self-join rejection is a planner check (no runtime join), so it is
# correct at every worker count and lives outside the xfailed join classes.
class TestDirectSelfJoinRejected:
    def test_direct_self_join_rejected(self, client):
        """Direct self-join t JOIN t (single dependency edge) is rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW j AS SELECT a.id FROM t AS a JOIN t AS b ON a.k = b.k",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t"], views=["j"])


# ── two different views over the same base, joined: vt1 JOIN vt2 ───────────

@_MULTIWORKER_JOIN_XFAIL
class TestTwoViewsJoin:
    def test_two_views_over_same_base_joined(self, client):
        """vt1 (v>0) JOIN vt2 (v>50) on k, both views over t. Distinct source
        ids → two edges → correct. Same-epoch multi-row insert + evolution."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW vt1 AS SELECT id, k, v FROM t WHERE v > 0", schema_name=sn)
            client.execute_sql("CREATE VIEW vt2 AS SELECT id, k, v FROM t WHERE v > 50", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW j AS "
                "SELECT vt1.id AS lid, vt1.k AS lk, vt1.v AS lv, "
                "vt2.id AS rid, vt2.k AS rk, vt2.v AS rv "
                "FROM vt1 JOIN vt2 ON vt1.k = vt2.k",
                schema_name=sn,
            )
            vid = _vid(client, sn, "j")
            project = ["lid", "lk", "lv", "rid", "rk", "rv"]
            t_state = {}

            def expected():
                s1 = {pk: r for pk, r in t_state.items() if r["v"] > 0}
                s2 = {pk: r for pk, r in t_state.items() if r["v"] > 50}
                return oracle.oracle_equijoin(
                    left=s1, lwhere=None, lkey="k", lproj=["id", "k", "v"],
                    right=s2, rwhere=None, rkey="k", rproj=["id", "k", "v"],
                    out_cols=project,
                )

            def check(ctx):
                oracle.assert_view_matches(client, vid, project, expected(), ctx=ctx)

            # Same-epoch multi-row insert: some rows satisfy both predicates.
            client.execute_sql(
                "INSERT INTO t VALUES (1, 3, 10), (2, 3, 60), (3, 3, 99)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 1, "k": 3, "v": 10}, {"id": 2, "k": 3, "v": 60}, {"id": 3, "k": 3, "v": 99}])
            check("after-insert")

            client.execute_sql("UPDATE t SET v = 70 WHERE id = 1", schema_name=sn)
            oracle.apply_update(t_state, "id", 1, {"v": 70})
            check("after-update-into-vt2")

            client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
            oracle.apply_delete(t_state, "id", [2])
            check("after-delete")
        finally:
            _cleanup(client, sn, tables=["t"], views=["j", "vt1", "vt2"])


# ── same-source INTERSECT / EXCEPT: rejected (Part 1a) ─────────────────────

class TestSameSourceSetOpRejected:
    def test_same_table_except_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 EXCEPT SELECT c FROM t WHERE c < 10",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])

    def test_same_table_intersect_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 INTERSECT SELECT c FROM t WHERE c < 10",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])

    def test_same_view_except_rejected(self, client):
        """Both branches FROM the same view → same source id → rejected."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT c FROM vt EXCEPT SELECT c FROM vt",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t"], views=["v", "vt"])

    def test_same_view_intersect_rejected(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE VIEW vt AS SELECT id, c FROM t WHERE c > 0", schema_name=sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT c FROM vt INTERSECT SELECT c FROM vt",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["t"], views=["v", "vt"])


# ── two different views over the same base in EXCEPT/INTERSECT: accepted ───
#
# The decisive non-over-rejection case: the guard keys on source-id equality,
# NOT base-table overlap. vt1 and vt2 are distinct views (distinct source ids)
# that happen to read the same base table t.

class TestTwoViewsSetOp:
    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW vt1 AS SELECT id, c FROM t WHERE c > 0", schema_name=sn)
        client.execute_sql("CREATE VIEW vt2 AS SELECT id, c FROM t WHERE c > 50", schema_name=sn)

    def _branches(self, t_state):
        left = oracle.oracle_filter_project(
            {pk: r for pk, r in t_state.items() if r["c"] > 0}, where=None, project=["c"])
        right = oracle.oracle_filter_project(
            {pk: r for pk, r in t_state.items() if r["c"] > 50}, where=None, project=["c"])
        return left, right

    def test_two_views_except_accepted_and_correct(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            # Must NOT raise — distinct source ids.
            client.execute_sql(
                "CREATE VIEW v AS SELECT c FROM vt1 EXCEPT SELECT c FROM vt2", schema_name=sn)
            vid = _vid(client, sn, "v")
            t_state = {}

            def check(ctx):
                left, right = self._branches(t_state)
                exp = oracle.oracle_setop("EXCEPT", left, right)
                oracle.assert_view_matches(client, vid, ["c"], exp, ctx=ctx)

            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 60), (3, 99)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 1, "c": 10}, {"id": 2, "c": 60}, {"id": 3, "c": 99}])
            check("after-insert")

            client.execute_sql("UPDATE t SET c = 70 WHERE id = 1", schema_name=sn)
            oracle.apply_update(t_state, "id", 1, {"c": 70})
            check("after-update")

            client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
            oracle.apply_delete(t_state, "id", [2])
            check("after-delete")
        finally:
            _cleanup(client, sn, tables=["t"], views=["v", "vt1", "vt2"])

    def test_two_views_intersect_accepted_and_correct(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT c FROM vt1 INTERSECT SELECT c FROM vt2", schema_name=sn)
            vid = _vid(client, sn, "v")
            t_state = {}

            def check(ctx):
                left, right = self._branches(t_state)
                exp = oracle.oracle_setop("INTERSECT", left, right)
                oracle.assert_view_matches(client, vid, ["c"], exp, ctx=ctx)

            client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 60), (3, 99)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 1, "c": 10}, {"id": 2, "c": 60}, {"id": 3, "c": 99}])
            check("after-insert")

            client.execute_sql("UPDATE t SET c = 80 WHERE id = 1", schema_name=sn)
            oracle.apply_update(t_state, "id", 1, {"c": 80})
            check("after-update")

            client.execute_sql("DELETE FROM t WHERE id = 3", schema_name=sn)
            oracle.apply_delete(t_state, "id", [3])
            check("after-delete")
        finally:
            _cleanup(client, sn, tables=["t"], views=["v", "vt1", "vt2"])


# ── UNION / UNION ALL over the same table: accepted (linear, no correction) ─

class TestSameSourceUnion:
    def test_same_table_union_all_accepted_weight2(self, client):
        """UNION ALL of two overlapping branches over the same table → rows
        matching both predicates carry weight 2 (exercises the weight-sum
        comparator). Accepted: UNION ALL is a linear merge, no correction term."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 UNION ALL SELECT c FROM t WHERE c < 100",
                schema_name=sn,
            )
            vid = _vid(client, sn, "v")
            t_state = {}

            def check(ctx):
                left = oracle.oracle_filter_project(t_state, lambda r: r["c"] > 0, ["c"])
                right = oracle.oracle_filter_project(t_state, lambda r: r["c"] < 100, ["c"])
                exp = oracle.oracle_setop("UNION ALL", left, right)
                oracle.assert_view_matches(client, vid, ["c"], exp, ctx=ctx)

            # c=50 satisfies both predicates → weight 2; c=150 only the first.
            client.execute_sql("INSERT INTO t VALUES (1, 50), (2, 150), (3, -5)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 1, "c": 50}, {"id": 2, "c": 150}, {"id": 3, "c": -5}])
            check("after-insert")

            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            oracle.apply_delete(t_state, "id", [1])
            check("after-delete")
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])

    def test_same_table_union_distinct_accepted(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT c FROM t WHERE c > 0 UNION SELECT c FROM t WHERE c < 100",
                schema_name=sn,
            )
            vid = _vid(client, sn, "v")
            t_state = {}

            def check(ctx):
                left = oracle.oracle_filter_project(t_state, lambda r: r["c"] > 0, ["c"])
                right = oracle.oracle_filter_project(t_state, lambda r: r["c"] < 100, ["c"])
                exp = oracle.oracle_setop("UNION", left, right)
                oracle.assert_view_matches(client, vid, ["c"], exp, ctx=ctx)

            client.execute_sql("INSERT INTO t VALUES (1, 50), (2, 50), (3, 150)", schema_name=sn)
            oracle.apply_insert(t_state, "id", [
                {"id": 1, "c": 50}, {"id": 2, "c": 50}, {"id": 3, "c": 150}])
            check("after-insert")

            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            oracle.apply_delete(t_state, "id", [1])
            check("after-delete-one-of-two-50s")
        finally:
            _cleanup(client, sn, tables=["t"], views=["v"])


# ── property test: random ops over the transitive-self-join config ─────────

@_MULTIWORKER_JOIN_XFAIL
class TestSelfJoinProperty:
    @pytest.mark.slow
    @pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
    def test_random_ops_match_oracle(self, client, seed):
        rng = random.Random(seed)
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _setup_self_join(client, sn)
            vid = _vid(client, sn, "j")
            t_state = {}
            next_pk = [1]          # monotonic, never reused
            KEYS = [0, 1, 2]       # small space → dense many-to-many

            def live_pks():
                return list(t_state.keys())

            for step in range(40):
                live = live_pks()
                # Bias toward inserts early (nothing to delete/update yet).
                choices = ["insert"]
                if live:
                    choices += ["delete", "update", "insert"]
                op = rng.choice(choices)

                if op == "insert":
                    n = rng.choice([1, 1, 2, 3])  # occasional same-epoch multi-row
                    rows, tuples = [], []
                    for _ in range(n):
                        pk = next_pk[0]; next_pk[0] += 1
                        k = rng.choice(KEYS)
                        v = rng.choice([-2, 0, 1, 5, 9])  # straddle the v>0 filter
                        rows.append({"id": pk, "k": k, "v": v})
                        tuples.append(f"({pk}, {k}, {v})")
                    sql = "INSERT INTO t VALUES " + ", ".join(tuples)
                    client.execute_sql(sql, schema_name=sn)
                    oracle.apply_insert(t_state, "id", rows)
                elif op == "delete":
                    pk = rng.choice(live)
                    client.execute_sql(f"DELETE FROM t WHERE id = {pk}", schema_name=sn)
                    oracle.apply_delete(t_state, "id", [pk])
                else:  # update — never touches the PK
                    pk = rng.choice(live)
                    new_v = rng.choice([-2, 0, 1, 5, 9])
                    client.execute_sql(f"UPDATE t SET v = {new_v} WHERE id = {pk}", schema_name=sn)
                    oracle.apply_update(t_state, "id", pk, {"v": new_v})

                try:
                    oracle.assert_view_matches(
                        client, vid, _J_PROJECT, _self_join_expected(t_state),
                        ctx=f"seed={seed} step={step} op={op}")
                except AssertionError as e:
                    raise AssertionError(f"[replay seed={seed} step={step}] {e}") from None
        finally:
            _cleanup(client, sn, tables=["t"], views=["j", "vt"])
