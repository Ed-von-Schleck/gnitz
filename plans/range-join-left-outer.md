# LEFT OUTER range / band join

## Goal

`CREATE VIEW … AS SELECT … FROM a LEFT JOIN b ON <eq…> AND <range>` is rejected
today — `build_join_view` bounces any range conjunct under a LEFT join
(`planner.rs:1083`, *"LEFT JOIN with a range predicate is not supported"*). Only
INNER range / band joins exist. This plan adds the LEFT variant: every left row
with ≥1 range match emits the matched pairs (already produced by the INNER
circuit); every left row with **no** match emits one null-filled row
(`[a cols…, NULL b cols]`, weight = `w_a`), maintained incrementally under both
ΔA and ΔB and across the distributed broadcast/scatter.

The whole feature is a planner change inside `build_range_join_view`
(`planner.rs:1335`) plus the one-line rejection removal — **no new engine
operator and no new builder method**. The null-fill is a pure **Z-set
difference**: the left input minus the matched left rows, where the matched left
rows (*with their full payload*) are read straight off the INNER join's own
output. The only non-linear node is one `distinct`; everything else is `map`,
`union`, `negate`, `shard`, `null_extend` — all linear.

```sql
-- band LEFT (n_eq ≥ 1): a-rows with no b in [a.lo, ∞) on their k-group → (a, NULL)
CREATE VIEW v AS SELECT a.id, b.id FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t;
-- pure-range LEFT (n_eq = 0): match set spans the whole table, scattered over all
-- workers → the unmatched test is global, reconciled by the a-PK exchange (§3)
CREATE VIEW w AS SELECT a.id, b.id FROM a LEFT JOIN b ON a.x < b.y;
```

## 1. The matched set is keyed by left identity, not by key

The equi LEFT join (`build_join_view`, `planner.rs:1214-1242`) tracks matches as
`distinct(B_keys)`, keyed by the **join key**: for an equi-join "`a` has a match"
⟺ "`a`'s join key ∈ B's key set", a function of **B alone**.

A range / band join has no such B-only set. The match relation is
`match(a, b) = eq(a)=eq(b) ∧ rel(a.range, b.range)`: a single left row matches a
whole **interval** of B's range slots. Two left rows in the same eq-group with
different range bounds have different intervals and different match status, so no
key-set membership captures "has a match". The matched set must therefore be the
set of **left identities** that actually appear in the inner join output, keyed by
the left row's own source PK `a.pk` (unique per left row):

```
M = π_a( A ⋈θ B )          // left-identity-keyed: a.pk ↦ how many live matches a has
```

Because `M` is derived from `A` (through the inner join), its delta `ΔM` is
non-zero in the **same epoch** as `ΔA`. The equi path's "ΔA and ΔB land in
disjoint epochs" simplification is gone. The naive response — compute a key-only
`distinct(M)` and anti-join `A` against it — re-introduces that simultaneity at the
anti-join/join layer (where the engine reads *delayed* traces and a delta-delta
cross term becomes unavoidable). §2 avoids the whole problem instead.

## 2. The null-fill is a set difference, and `distinct` absorbs the simultaneity

The null-fill is, by definition, the anti-join of the left input against the
matched set:

```
nullfill = antijoin(A, M) = A − semijoin(A, M)
```

The key move: **`semijoin(A, M)` — the matched left rows *with payload* — is
already computed.** It is the a-side of the inner output, deduplicated per `a.pk`.
The inner output carries every A column verbatim, so there is nothing to recover
by joining; we just project and dedup:

```
D = distinct( π_{A-cols}( A ⋈θ B ) )       // matched left rows, full A payload, one per a.pk
nullfill = A − D                            // pure Z-set subtraction
```

`A` and `D` are both keyed by `a.pk` and `D ⊆ A` (every matched left row is a left
row), so `A − D` is exactly the unmatched left rows, never negative: a matched `a`
is `+1` in `A` and `+1` in `D` → cancels; an unmatched `a` is `+1` in `A`, absent
from `D` → survives. Realized as `union(A, negate(D))` — **linear**.

### Why no cross term, no delayed-trace reasoning

Incrementally, `Δnullfill = ΔA − ΔD`. Both are deltas available in the same epoch;
the subtraction is linear, so there is **no trace read at the subtraction layer**
to be "current" or "delayed". The only non-linear node is the `distinct` producing
`D`, and `distinct` *natively* computes "delayed integral + this epoch's delta →
net transition" (DBSP Prop 4.7: point lookups into its own integral detect the
positive-boundary crossing; `op_distinct`, confirmed by `test_op_distinct_boundary`
— 0→positive emits +1, positive→0 emits −1, positive→positive nothing). So when a
new left row matches in the *same* epoch it is inserted, `distinct` sees that
match in this epoch's inner delta and emits `ΔD = +a` in the *same* epoch — exactly
cancelling `ΔA = +a`. The simultaneity is handled inside the one operator built to
handle it; outside it, everything is linear.

### Case table (`nf` ≜ null-fill `(a, NULL)`)

| event | `ΔA` (left input) | `ΔD = Δ distinct(π_A(inner))` | `Δnullfill = ΔA − ΔD` |
|---|---|---|---|
| ΔA insert, matches this epoch | +a | +a (inner emits a's match; distinct 0→1) | **0 ✓** |
| ΔA insert, no match | +a | 0 (no inner row for a) | +a ✓ |
| ΔA delete, was unmatched | −a | 0 | −a ✓ |
| ΔA delete, was matched | −a | −a (a's matches retract; distinct 1→0) | **0 ✓** |
| ΔB, a gains 1st match | 0 | +a (inner BA term; distinct 0→1) | −a ✓ |
| ΔB, a loses last match | 0 | −a (inner BA term; distinct 1→0) | +a ✓ |
| ΔB, a gains 2nd match | 0 | 0 (distinct 1→2, no crossing) | 0 ✓ |
| ΔB, a loses 1 of 2 | 0 | 0 (distinct 2→1, no crossing) | 0 ✓ |

The two bolded rows are the same-epoch cases that defeat a key-only anti-join; here
they fall out of `distinct`'s ordinary behavior with no extra machinery. Match
multiplicity (rows 7–8) is handled because `distinct` tracks net weight per a.pk
and only emits on the 0-boundary crossing.

## 3. Distribution — the a-PK exchange reconciles cross-worker matches

The INNER circuit broadcasts (pure range, `n_eq == 0`) or eq-prefix-scatters
(band, `n_eq ≥ 1`) the input and exchanges its output on the source-PK **pair**
`(a.pk, b.pk)` (`planner.rs:1500-1505`). A left row's matches are spread across
workers — over the whole key space for a pure range join, across the eq-group's
worker for a band join. "Does `a` have any match" is a **global** question.

Both sides of the difference are computed in `a.pk` space:

- `D`: project the inner output to the A columns, **re-exchange by `a.pk`**, then
  `distinct`. The exchange gathers every one of `a`'s matches (from every worker,
  from both the AB and BA inner terms) onto the single worker that owns `a.pk`,
  where `distinct` decides existence partition-locally.
- `A`: re-shard the left input by `a.pk`. Under the pure-range broadcast every
  worker holds the full left delta and the shard keeps only its `a.pk`-owned slice;
  under the band scatter each left row sits on its eq-prefix worker and the shard
  re-routes it to its `a.pk` worker. Either way each left row lands once on its
  `a.pk` owner, co-partitioned with `D`.

`union(A, −D)` then runs partition-locally; only the final re-key + shard moves the
null-fills back to pair-PK space to union with the inner output.

`π_a` weights sum correctly per `a.pk` *before* `distinct`: a left row matching `b1`
and `b2` accumulates weight 2 at `a.pk`, a retraction of one match drops it to 1
(still "present"), and only when the last match retracts does `distinct` cross to 0.
The pair-PK byte-identity the inner output needs for its own ±1 cancellation is
irrelevant here — projecting to the A columns drops `b`, so an AB-emitted `+a` and a
later BA-retracted `−a` for the same left row collapse to the same `(a.pk, A)`
element and net in `distinct` regardless of which `b` carried them.

## 4. Output schema, null-fill PK, NULL keys

**Pair-PK with a sentinel `b.pk`.** The view PK is the source-PK pair
`[a.pk…, b.pk…]` (`planner.rs:1462-1464`). A null-fill row has no `b`, so re-keying
`[a cols…, NULL b cols…]` onto the pair-PK packs the NULL `b.pk` columns to the
synthetic `0` (`map_reindex`'s NULL-key rule, the same one the equi path relies on
at `planner.rs:1140-1142`). The null-fill's PK is thus `(a.pk, 0…)` with the `b`
payload columns marked NULL in the bitmap. It never `(PK,payload)`-merges with a
real `(a.pk, b.pk=0…)` inner row because `compare_rows` treats NULL ≠ 0
(`planner.rs:1172`). `a.pk` is unique in A, so distinct left rows get distinct
null-fill PKs.

**Byte-exact cancellation is load-bearing (§8).** A matched left row's `+1` (from
`A`) and `−1` (from `D`) must be byte-identical in `(PK, payload)` so they consolidate
to zero — including after `null_extend` → re-key → projection, which both copies
traverse identically. §5 guarantees this by sending *both* `A` and the inner
projection through `map_reindex` with the **same self-derive `a.pk` target types and
the same A-column copy**, so the `(a.pk, A)` bytes are identical by construction
(the only difference is the input addressing, not the output encoding).

**NULL join keys need no special case.** A left row with a NULL in any eq or range
column can never satisfy the predicate (SQL 3VL), so the inner join's existing NULL
filter (`planner.rs:1410-1412`) keeps it out of the inner output and therefore out
of `D`. But it is still a left row, so it is in `A` → `A − D` null-fills it
automatically. `a.pk` is non-nullable (PK columns always are), so re-keying `A` by
`a.pk` is always well-defined even when a join-key column is NULL; the NULL value
rides along as an ordinary (NULL-marked) A payload column. **No `input_a_match` /
`input_a_null` split, no bypass union** — the difference subsumes it.

## 5. The circuit, by builder calls

Inserted into `build_range_join_view` after `let merged = cb.union(proj_ab_node,
proj_ba_node);` (`planner.rs:1445`), replacing the unconditional re-key/project/
shard/sink tail (`planner.rs:1466-1506`) with an `is_left_join` split. `merged`
keeps its `[_join_pk×k, A, B]` layout (PK region = the `_join_pk` range key, payload
= `[A cols, B cols]`). Names match the existing code; `input_a_raw` is the left
input *before* the inner join's NULL filter.

```
INNER (unchanged):
    rekey = map_reindex(merged, pair_pk_cols)          // [pair-PK, _join_pk×k, A, B]
    projected = map(rekey, final_projection)           // [pair-PK, user cols]
    sink(shard(projected, pair_pk_idxs))

LEFT (added) — nullfill = A − distinct(π_A(inner)), all in a.pk space:
    // D: matched left rows (full A payload), gathered & deduped per a.pk
    proj_a  = map_reindex(merged, a_pk_in_merged, [0;pa], a_cols_prog)  // [a.pk, A] — drops B and _join_pk
    matched = distinct(shard(proj_a, 0..pa))           // a.pk EXCHANGE (§3); each matched a once, weight 1

    // A: every left row (incl. NULL-key), produced with the IDENTICAL [a.pk, A] encoding
    a_all   = shard(map_reindex(input_a_raw, left.pk_cols, [0;pa], build_reindex_program(left)), 0..pa)

    // A − D  (pure Z-set subtraction; distinct already resolved the ΔA/ΔB simultaneity)
    nullfill = null_extend(union(a_all, negate(matched)), right_col_tcs)  // [a.pk, A, NULL B]

    // back to pair-PK space, union with the inner output
    nf_rekey = map_reindex(nullfill, nf_pair_pk_cols, [0;pair_pk], reindex_prog(ab_schema))
    nf_proj  = map(nf_rekey, nf_projection)            // [pair-PK, user cols], B NULL
    sink(union(shard(projected, pair_pk_idxs), shard(nf_proj, pair_pk_idxs)))
```

Index/schema derivations (all mechanical, no new helper):

- `a_pk_in_merged` = `[k + c for c in left.pk_cols]` — A's PK columns sit in the A
  payload region of `merged`, after the `k` `_join_pk` slots (the same index the
  inner pair-PK re-key uses for its A-PK half).
- `a_cols_prog` — a reindex program that copies **only the A columns** of `merged`
  (payload positions `k..k+left_n`) into the output payload, dropping the `_join_pk`
  slots and the B columns. Re-keying by `a_pk_in_merged` with `[0;pa]` (self-derive)
  yields `[a.pk, A]`. (`build_reindex_program(union_schema)` would keep B, breaking
  the per-`a.pk` dedup — `distinct` must see `b` already dropped. The A-only program
  is a trivial `ExprBuilder` copy of the `left_n` A columns; equivalently, re-key
  with the full program and follow with a `map` that selects the A region — no new
  helper either way.)
- `a_all` uses `build_reindex_program(left)` (copy all left columns as payload) and
  re-keys by `left.pk_cols` with the same `[0;pa]` self-derive. Its `(a.pk, A)`
  output is byte-identical to `proj_a`'s for matched rows: same a.pk values and
  types → same self-derived `reindex_output_type` → same OPK bytes, and A columns
  copied verbatim → same payload bytes. This symmetry is the cancellation invariant
  (§4, §8).
- `ab_schema` = `left.columns ++ right.columns` (the `nullfill` payload layout
  `[A, B]`, B columns NULL). `nf_pair_pk_cols` = `[c for c in left.pk_cols] ++
  [left_n + c for c in right.pk_cols]` — a.pk from the A region, b.pk from the NULL
  B region (packs to 0).
- `right_col_tcs` = `right.columns.iter().map(|c| c.type_code as u64)` — the same
  null-extend type list the equi path builds (`planner.rs:1136-1137`).
- `nf_projection` = `build_join_view_projection(... payload_offset = pair_pk ...)`
  — identical user columns to the inner `final_projection` but offset by `pair_pk`
  not `pair_pk + k` (null-fills carry no `_join_pk` slots). Same `final_cols`, so
  both branches agree on the output schema; assert it.

No scheduling constraints: the null-fill core is a straight chain
(`distinct → negate → union → null_extend → …`); the only fan-outs are `merged`
(→ inner re-key + `proj_a`) and `input_a_raw` (→ inner NULL-filter + `a_all`),
both read by non-destructive `map_reindex`/`filter`. The destructive-register
ordering rule (`planner.rs:2871-2874`) does not bite, because the one destructive
node (`distinct`, which empties its input register) is the sole reader of
`shard(proj_a)`.

## 6. Planner wiring

- **Remove the rejection** (`planner.rs:1078-1086`): drop the `if is_left_join {
  return Err(…) }` guard inside the `if let Some(range) = range_conjunct` arm and
  pass `is_left_join` into `build_range_join_view`.
- **Thread `is_left_join: bool`** through `build_range_join_view`'s signature
  (`planner.rs:1335-1350`). The INNER path is byte-identical when `false`.
- **Keep a handle to the unfiltered left input.** The inner join still filters
  NULL keys off its own reindex input (`planner.rs:1410-1412`); the LEFT null-fill
  taps `input_a_raw` (the `input_delta_tagged` node, before that filter) for `a_all`
  so NULL-key rows are null-filled (§4). No `input_a_match`/`input_a_null` split.
  `input_b` keeps its plain non-null filter (an unmatched B row never appears in the
  inner output, hence never in `D`).
- **`final_cols` / `view_pk` are unchanged** from the INNER range join: the output
  schema, the `pair_pk` columns, and the `ExchangeShard` on the pair-PK are
  identical; LEFT only adds rows (the null-fills) into that same shape.
- **RIGHT / FULL OUTER stay rejected** for range exactly as for equi — only INNER
  and LEFT are matched at `planner.rs:1047-1055`; the `_ => Unsupported` arm is
  untouched. (A range RIGHT join is `b LEFT JOIN a` with the conjunct's sides
  swapped; the planner does not rewrite it, matching the equi boundary.)

## 7. Engine: no new operator, no new builder

Every node the null-fill uses already exists and is heavily exercised:
`map_reindex`, `shard`, `distinct` / `op_distinct`, `negate`, `union`, `null_extend`
/ `op_null_extend`, `map`. There are **no joins** in the null-fill path — no
anti-join, no delta-trace join, no delta-delta join — and **no extra trace tables**:
the only persistent state is `distinct`'s own integral (which the key-only matched
set would have needed anyway). The INNER range op `op_join_delta_trace_range` and its
`Join(DeltaTraceRange { n_eq, rel })` node are untouched — the LEFT join reuses the
inner output, it does not extend the range probe. The `AntiJoin`/`SemiJoin`
`DeltaTraceRange` wire variants stay `unreachable!` (`circuit.rs`).

The performance and robustness consequences of the set-difference formulation:

- **Cheaper per epoch.** No merge-walk joins (the dominant cost of the alternative);
  `distinct` is O(|delta|) point lookups, and the rest is linear scans. The
  alternative key-only-`M` decomposition needs three joins (an anti-join plus two
  payload-recovery joins, one of them a delta-delta cross term) and two extra trace
  tables written every epoch.
- **Less state.** One `distinct` trace (`a.pk` + A payload) versus that trace *plus*
  a re-integrated copy of the matched set *plus* a full A-integral.
- **No subtle timing.** The only non-linearity is inside `distinct`; there is no
  current-vs-delayed trace question and no destructive-register scheduling to get
  right (the engine reads all join-family traces delayed — `refresh_owned_cursors`
  snapshots them at epoch start from frozen `Rc<Batch>` clones — which is exactly
  what makes a join-based decomposition need a delta-delta cross term, and exactly
  what the set-difference sidesteps).

**foundations.md update (at implementation time).** foundations.md states the LEFT
outer join only as a per-delta-row null-fill (`:176-185`). Add the model-level
identity this plan uses (algebra only): a LEFT outer join is
`inner ∪ null_extend(A − distinct(π_A(inner)))`, where `π_A(inner)` carries the
preserved side's full payload, so the null-fill is a Z-set **difference**, not an
anti-join requiring payload recovery. The matched set is keyed by the **preserved
row's identity** (its source PK); for an equi join it may instead be keyed by the
join key as the B-only `distinct(B_keys)` optimization (reusing the join-key
exchange), but the identity-keyed difference is the general form and the only one
available when matches span an interval rather than a key. Because the lone
non-linear operator (`distinct`) absorbs the within-epoch ΔA/Δ-matched
simultaneity, no delta-delta cross term is needed despite the matched set depending
on the preserved side.

## 8. Correctness invariants to preserve

- **Byte-exact `(A, D)` cancellation.** A matched left row must be byte-identical in
  `A` and `D` so `union(A, −D)` consolidates it to zero (and stays zero through
  `null_extend` → re-key → projection). Guaranteed by routing both through
  `map_reindex` with identical self-derive `a.pk` targets and verbatim A-column
  copies (§5). Debug-assert that `proj_a` and the `a_all` re-key produce the same
  output schema; an E2E test with string / nullable / wide A payloads pins the byte
  equality (§9). This replaces the equi path's "null-fill `+1`/`−1` must match"
  concern and is the one genuinely new invariant.
- **Matched-set existence, not multiplicity.** `D = distinct(π_A(inner))` collapses
  per-`a.pk` match multiplicity to a 0/1 bit; an over-counted or tombstoned inner
  pair must not change `a`'s null-fill until the net count crosses 0 — guaranteed by
  `distinct` over the `a.pk`-summed weights (§3).
- **Null-fill identity is `(a.pk, 0…)` + NULL-b bitmap.** The `nf_rekey` program must
  pack the NULL `b.pk` columns to 0 and leave the `b` payload NULL, so a null-fill
  never merges with a real `b.pk = 0` row (NULL ≠ 0). Do not route null-fills through
  the inner `rekey`/`final_projection` (whose payload offset includes the `k`
  `_join_pk` slots the null-fills lack).
- **Schema agreement.** `nf_projection` and `final_projection` must yield the same
  `final_cols` (assert), and both branches shard on the identical `pair_pk_idxs`, or
  the post-shard `union` merges mismatched strides.
- **`D ⊆ A`.** Every inner row's left side is a (non-NULL-key) left row, so
  `A − D ≥ 0` everywhere — the difference can never manufacture a negative-weight
  null-fill. Relies on `a_all` being sourced from the *same* left input that feeds
  the inner join (`input_a_raw` ⊇ the NULL-filtered inner input).
- **Self-join stays rejected** independent of LEFT/range (`planner.rs:1064-1070`).

## 9. Testing

**Engine unit** — none; no operator or builder changes (the reused ops keep their
existing tests: `op_distinct`, `op_null_extend`, `op_union`, `op_map`/reindex).

**Circuit shape (`gnitz-sql` planner tests).** A LEFT range view builds, over the
INNER skeleton, the node set `{ map_reindex×3 (proj_a, a_all, nf_rekey), distinct,
negate, union×2, null_extend, map (nf_proj), shard×3 }`. Assert: the LEFT branch
adds **zero `Join`/`AntiJoin`/`SemiJoin` nodes and zero `integrate_trace`/`delay`
nodes** (`distinct` keeps its own integral internally — it is the only non-linear
node); exactly one `distinct`, fed by the `shard` of `proj_a`; `a_all` is a
`map_reindex` of the *unfiltered* left input.
INNER range views are byte-identical to before (`is_left_join == false`).

**Multi-worker E2E (`gnitz-py/tests/test_workers.py::TestRangeJoin`, `GNITZ_WORKERS
=4`).** A Python cross-filter reference computes the expected LEFT result (matched
pairs ∪ `(a, NULL)` for unmatched a). Add:

- **Band LEFT, ΔA then ΔB.** Seed `b` thin; insert `a` rows, some with no `b` in
  their `[lo, ∞)` band → assert `(a, NULL)` rows. Insert `b` rows that give a
  previously-unmatched `a` its first match → assert `(a, NULL)` is **retracted** and
  `(a, b)` appears. Delete that `b` → assert `(a, NULL)` returns.
- **Same-epoch insert-and-match.** In one epoch insert a left row whose match
  already sits in `b`'s trace → assert exactly one `(a, b)` pair and **no** net
  `(a, NULL)` (not even transiently in the emitted delta). This is the case that
  breaks a key-only anti-join; here `distinct` cancels it within the epoch.
- **Delete a matched row.** Reach steady state with `a` matched (one `(a, b)`, no
  null-fill), then delete `a` in one epoch → assert `(a, b)` retracts and **no**
  `(a, NULL)` is ever emitted.
- **Pure-range LEFT, cross-worker unmatched.** `n_eq == 0`, broadcast delta over a
  scattered `b`: a left row whose `x < b.y` matches nothing on *any* worker → one
  `(a, NULL)` (the a-PK exchange must gather the global empty match set, not emit a
  spurious null-fill per worker). A left row matching `b`s on several workers → the
  pairs, no null-fill.
- **Byte-identity / payload fidelity.** Use a left table with a STRING column, a
  nullable column carrying NULL, and (if available) a wide PK; insert matched rows
  and assert the null-fill set is empty (clean `A − D` cancellation) and the matched
  pairs carry the exact A payload. This pins the §8 byte-exact-cancellation invariant
  — a stride/encoding mismatch between `a_all` and `proj_a` would surface here as a
  spurious leftover `(a, NULL)`.
- **NULL join key.** A left row with NULL eq or NULL range column → exactly one
  `(a, NULL)`, never matched, never in `D` (no bypass branch exists).
- **Retraction / multiplicity.** Delete one of two `b` matches for an `a` → `a` stays
  matched (no null-fill); delete the last → `(a, NULL)` appears.

## 10. Scope boundaries (by design, not deferred)

- **RIGHT / FULL OUTER range joins** are rejected, matching the equi join's
  INNER+LEFT-only support (`planner.rs:1047-1055`). Not in scope.
- **Multiple range conjuncts / residual ON predicates** remain rejected and are a
  separate plan item (`wide-pk-incremental-views.md` §1): a LEFT join with a residual
  predicate would null-fill on the residual too, which the post-join `Filter` plan
  must handle. One eq-prefix + one range conjunct only, as for INNER.
- **Pure-range broadcast cost** is unchanged: the LEFT path adds one a-PK exchange
  of the (already-broadcast) left delta and of `π_a(inner)`; it does not alter the
  inner broadcast, which is the separate distribution item
  (`wide-pk-incremental-views.md` §1). The whole null-fill computation is
  partition-local on the a.pk shard after that exchange — no joins, no further
  exchange.
- **No range anti/semi/outer operator.** The outer semantics are a set difference
  over `distinct(π_a(inner))`, reusing the existing inner output and linear ops; the
  range probe is reused, not extended.
