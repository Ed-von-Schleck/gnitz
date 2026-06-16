# Band LEFT join: weight-exact null-fill via the per-eq-group threshold form

## Goal

The band LEFT join (`n_eq ≥ 1` eq prefix + one range conjunct) builds its null-fill as
`A − distinct(π_A(inner))` (`planner.rs:1693-1705`), which **over-fills bag-valued
preserved rows**: a matched weight-`w` left row emits a spurious weight-`(w−1)`
`(left, NULL)`. It is the lone LEFT-join null-fill that is not weight-exact — equi
(anti-join by key) and pure-range LEFT (`n_eq == 0`, the inline threshold form at
`planner.rs:1577-1680`) both are. The bug is **latent — not SQL-reachable** (§2), so
this is a weight-exactness / robustness parity fix, not live data corruption.

The fix reformulates band LEFT to the **per-eq-group subtraction-threshold form**,
generalizing the already-implemented pure-range threshold construction from
`n_eq == 0` to `n_eq ≥ 1`. The witness is `M_eq = MAX/MIN(b.range)` **per eq-group**;
null-fill = `int_a − (int_a ⋈ M_eq)`. This is weight-exact, simultaneity-correct, and
**partition-local** — band scatters by the eq prefix, so the per-group reduce needs no
broadcast and no `W×` state (band is the *easier* distributional case than pure-range).
It also collapses all three LEFT-join null-fills (equi anti-join by key, band /
pure-range threshold) onto the single weight-exactness principle of foundations §3.

## 1. The bug

The `else` (`n_eq ≥ 1`) branch of the LEFT null-fill, `planner.rs:1693-1705`:

```rust
let proj_a  = cb.map(rekey_a, &a_cols);     // [a.pk, A], matched left rows read off inner
let matched = cb.distinct(proj_a);          // [a.pk, A], CLAMPED to ±1
let a_all   = cb.map_reindex(input_a_raw, &left_schema.pk_cols, &zero_a, ...); // [a.pk, A], weight w
let neg = cb.negate(matched);
cb.union(a_all, neg)                        // a_all − matched
```

A matched preserved row of weight `w`: `a_all = w`, `distinct(matched) = 1`, so
`nf = w − 1` — a spurious weight-`(w−1)` null-fill (should be `0`). The clamp is in the
subtraction: `distinct` caps the matched multiplicity at 1 while `a_all` keeps `w`.
(This is foundations §3's "`distinct` clamps multiplicity" — exact only for a
unique-identity preserved side.)

## 2. Reachability: LATENT (not SQL-reachable)

No SQL surface delivers a weight-≥2 (same PK, same payload) row to a band join's left
input. Every multiplicity path collapses to weight-1, *and* content-colliding paths
re-key to distinct PKs:

- **Base tables are always `unique_pk`.** SQL `CREATE TABLE` hardcodes the flag:
  `execute_create_table` calls `client.create_table(.., &pk_indices, true, ..)`
  (`planner.rs:501`). `enforce_unique_pk` collapses same-PK accumulation; the DML
  validator rejects `w > 1` on a unique-PK table. The non-`unique_pk` flavor exists
  only on the raw `gnitz-core`/`gnitz-py` client.
- **`UNION ALL` views branch-tag.** `compile_set_op_side` keys each side via
  `map_hash_row(.., branch_id)` (`planner.rs:2946`), a content hash mixed with
  `branch_id`; `(Union, All)` sets the right side's `branch_id = 1`
  (`planner.rs:3056-3062`), so two identical payloads hash to **distinct** PKs → two
  weight-1 rows, never one weight-2 row.
- **Projection views force-include or reject the PK** (`build_projection`), and
  `map_reindex` keys by the listed columns, never a content hash — so two distinct
  source rows never collapse onto one PK at weight 2. GROUP BY (one row/group),
  DISTINCT (clamped), and joins (PK-pair keyed) are all weight-1.

So the bug triggers **only** via the low-level client (`create_table(unique_pk=false)`
+ a weighted batch `push`), then `CREATE VIEW … band LEFT JOIN` over it. Severity:
weight-exactness parity / defense-in-depth.

## 3. The naive anti-join fix is WRONG (record this)

The tempting fix — mirror equi LEFT with an anti-join keyed by `[eq, range]`,
`matched_keys = distinct(map_key_only(merged))`, `nf = anti_join(reindex_a, I(matched_keys))`
plus a `negate(join(matched_keys, trace_a))` correction — **is incorrect**. Band-match
existence is **not** a function of `B` alone (it depends on the left row's own range
value), so `matched_keys` is derived from the inner output and is therefore entangled
with `ΔA`. A left row inserted in the **same epoch** it matches an *existing* `b`:

- `anti_join(reindex_a, z⁻¹(I(matched_keys)))` tests the new row against the **old**
  matched-keys trace, which does not yet contain its key → emits a **spurious null-fill**;
- the `negate(join(Δmatched_keys, z⁻¹(trace_a)))` correction joins the new key against
  the **old** `A` trace, which has no row with that key → emits nothing.

Net: `+(a, b)` (inner) **and** `+(a, NULL)` (spurious), never retracted. Equi LEFT
avoids this only because `distinct(B keys)` is `ΔB`-driven and stable in a `ΔA` epoch,
so its anti-join tests against the *current* witness. Band's witness must likewise be
**right-side only** — which the threshold form provides (`M_eq` is a `ΔB`-driven
reduce, stable in a `ΔA` epoch, integrated so the `int_a ⋈ trace_M` join sees the
current `M_eq`).

The current `a_all − distinct(proj_a)` form is, by contrast, simultaneity-*correct*
(`distinct(proj_a)` is a delta op that sees the new match in-epoch, netting
`+a − a = 0`); its only defect is the weight clamp. The fix must preserve
simultaneity-correctness **and** add weight-exactness — which the threshold form does,
and the naive anti-join does not.

## 4. The fix — per-eq-group threshold (generalize the implemented pure-range form to `n_eq ≥ 1`)

Match existence collapses to a per-eq-group threshold: `a` matches ⟺
`a.range OP M_{a.eq}` where `M_{eq} = MAX(b.range)` (for `<`/`<=`) or `MIN` (for
`>`/`>=`) over `b` in eq-group `eq`. The null-fill is the subtraction form
(foundations §3, empty-other-side robust; weight-exact via the one-row-per-group
witness):

```
M_eq      = reduce_multi_local(reindex_b, group=[eq prefix], [(MAX/MIN, range)])  // per-eq-group threshold
trace_M   = integrate_trace(reindex_M)        // keyed [eq], one row/group, partition-local (NO broadcast)
matched   = (int_a ⋈ trace_M) ∪ (Δm ⋈ trace_a)   on range.op, carrying n_eq       // weight-exact (one row/group)
nf_match  = int_a − matched                                                        // union(int_a_keyed, negate(matched))
nf_null   = the existing NULL-key branch
out       = union(inner, null_extend(nf_match ∪ nf_null) → rekey pair-PK) → shard → sink
```

This is structurally identical to the implemented pure-range threshold form
(`planner.rs:1577-1680`), with three differences, all because `n_eq ≥ 1`:

1. **The reduce groups by the eq prefix** (`group_cols = [eq cols]`), not the empty
   set, so it produces one `M` *per eq-group* rather than one global `m`. (Pure range
   uses a synthetic single-group key; band uses the eq-prefix key.)
2. **The threshold join carries `n_eq`** (`join_with_trace_range_node(.., n_eq, ..)`,
   `planner.rs:1624-1625` carries `0` today): it matches on the eq prefix then
   range-compares, exactly like the inner band terms.
3. **Distribution is partition-local — no broadcast, no `W×`.** The band relay
   scatters `b` and `a` by the eq prefix (`prepare_relay`, `master.rs:827-830`,
   `n_eq ≥ 1` → eq-prefix scatter), so each eq-group's `b`, its `M_eq`, and the `a`s
   sharing that eq-prefix are **co-located on one worker**. The reduce is a normal
   distributed GROUP BY (1× state, one `M` per group on its owner) and `trace_M` is
   already eq-partitioned like `trace_a`/`trace_b` — no `partition_filter` issue.
   Pure range, by contrast, broadcasts and recomputes one global `m` redundantly on
   every worker; band avoids that entirely.

### 4.1 Shared machinery (already in the codebase)

Every primitive this fix needs already exists, built for the inline pure-range path:

- **Shard-free reduce.** `reduce_multi` auto-inserts an `ExchangeShard`
  (`circuit.rs:465`); since `b` is already eq-scattered, the reduce must be shard-free
  or `v_left` gains a second exchange and mis-compiles. Reuse `reduce_multi_local`
  (`circuit.rs:482`) — the same shard-free builder the pure-range block uses. (Here
  the input is eq-*scattered* rather than broadcast, but the requirement — no
  auto-shard — is identical.) The `m`-construction `map_hash_row → reduce_multi_local
  → reindex_m → integrate_trace` (`planner.rs:1609-1613`) generalizes by adding the
  eq-group cols to the reduce and the eq prefix to `reindex_m`.
- **Subtraction null-fill + re-key tail.** `nf_match = int_a − matched` via
  `negate`+`union`, then the re-key `[eq, range] → [a.pk…, A]` (the pure-range re-key
  closure at `planner.rs:1657-1660`, generalized from `k = 1` to `k = n_eq + 1`), then
  the shared `null_extend → nf_rekey → nf_proj → union(inner) → shard → sink` tail
  (`planner.rs:1708` onward), unchanged.
- **NULL-key branch.** A NULL eq/range key is filtered out of `reindex_a`/`trace_a`
  (3VL); the pure-range path routes it once via `filter(input_a_raw) → map_reindex →
  partition_filter` (`planner.rs:1671-1677`). Band can reuse this directly. Pin it with
  a multi-worker NULL-key test.

### 4.2 Exact planner edits

- **Remove** the band set-difference block (`planner.rs:1693-1705`: the
  `rekey_a`/`proj_a`/`matched`/`a_all`/`neg`/`union`, including the `cb.distinct`).
- **Promote** the pure-range `n_eq == 0` threshold block (`planner.rs:1577-1680`) to a
  general `n_eq ≥ 0` builder: group the reduce by the eq prefix, build the threshold
  terms with `n_eq` (not `0`), and drop the broadcast-specific handling (band is
  eq-scattered). The cleanest structure is a single threshold builder parameterized by
  `n_eq`, used for both band and pure-range, replacing the `if n_eq == 0 { … } else
  { … }` split with one construction.
- Net: `build_range_join_view`'s LEFT path has **one** null-fill construction (the
  threshold form) for all `n_eq ≥ 0`, plus the NULL-key branch. The equi join
  (`execute_create_join_view`) keeps its anti-join-by-key form (no range conjunct; its
  witness `distinct(B keys)` is already right-side-only and weight-exact).

## 5. Correctness (weight arithmetic)

Matched weight-`w` left row `a` (eq=g, matches some `b`): `int_a = +w·a`;
`int_a ⋈_{a.range OP M_g} trace_M = +w·a` (one-row `M_g` per group cannot multiply, so
it carries `a`'s weight `w`); `nf = w·a − w·a = 0`. ✓ Unmatched weight-`w` (`a.range`
past `M_g`, or eq-group `g` empty so `M_g` absent): `matched = 0`, `nf = +w·a`. ✓
Empty `b` in group `g` ⇒ `trace_M` has no row for `g` ⇒ `matched = ∅` ⇒ every `a` in
`g` null-fills (foundations §3 empty-other-side robustness). Within-epoch
insert-and-match: `M_g` is `ΔB`-stable and integrated, so `int_a ⋈ trace_M` sees the
current `M_g` and nets `a` in the same epoch (§3). Threshold-move (`b` extreme deleted
in group `g`): `Δm ⋈ trace_a` re-tests that group's `a`s.

## 6. Testing

- **Bag weight-exactness (the regression).** Not SQL-constructible (§2), so a Rust
  engine-level or `gnitz-py` raw-client test: `unique_pk=false` left table, push a
  weight-2 row that band-matches; assert null-fill weight `0` (matched) and `2` (an
  unmatched weight-2 row). This test **fails** on the current `distinct` form and
  passes on the fix.
- **The full band LEFT suite stays green** (`test_workers.py`, `TestRangeJoin`
  `test_band_left_*`): `delta_a_then_b`, `same_epoch_match` (the simultaneity guard §3
  — matched-on-insert must yield no `(a,NULL)`), `delete_matched_row`,
  `cross_worker_eq_groups` (per-eq-group `M` across workers), `null_join_key`,
  `payload_fidelity_wide_pk`.
- **Empty / deleted eq-group.** A band LEFT where one eq-group's `b` is empty (all its
  `a`s null-fill) and another is deleted-to-empty (its `a`s re-null-fill) — the
  per-group analog of the pure-range empty-`b` tests.
- **Circuit-shape.** `test_band_left_join_circuit_shape` (`planner_join.rs:749`): the
  band view loses the `distinct`/`negate` set-difference and gains the threshold shape
  (a `Reduce` grouped by eq, `+2` range terms, one `ExchangeShard`) — converging on the
  same shape `test_pure_range_left_join_circuit_shape` (`planner_join.rs:575`) asserts.
  Update assertions to the threshold form; assert `OPCODE_DISTINCT == 0` and
  `OPCODE_REDUCE == 1` in the band LEFT view.

## 7. Scope

- **Only band LEFT (`n_eq ≥ 1`).** Equi LEFT (anti-join by key) and pure-range LEFT
  (the threshold form) are already weight-exact; INNER joins have no null-fill.
- **A direct generalization of existing code.** The pure-range threshold construction,
  its shard-free reduce, and the subtraction null-fill tail are all implemented
  (`planner.rs:1577-1680`); this fix extends that one construction to `n_eq ≥ 1` and
  deletes the band set-difference block, collapsing both LEFT range/band null-fills
  into one parameterized builder. No sequencing dependency remains.
- **Supersedes the band set-difference null-fill entirely** — the `distinct`-clamp
  defect disappears with it. Foundations §3 already documents the underlying principle
  (the exact form subtracts match existence while preserving multiplicity — here via
  the one-row-per-group witness).
