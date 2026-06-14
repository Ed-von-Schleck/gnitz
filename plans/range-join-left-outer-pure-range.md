# Pure-range LEFT OUTER join (no equality conjunct)

## Goal

Support `CREATE VIEW … AS SELECT … FROM a LEFT JOIN b ON <range>` with **no**
equality conjunct (`n_eq == 0`), e.g.:

```sql
CREATE VIEW w AS SELECT a.id, b.id FROM a LEFT JOIN b ON a.x < b.y;
```

INNER pure-range and band LEFT (`n_eq ≥ 1`) are implemented in
`build_range_join_view` (`planner.rs:1339`); pure-range LEFT is rejected at
`planner.rs:1371-1375`. This plan lifts that rejection by exploiting a structural
fact the "needs two exchanges in series" framing missed. The only additions are
**planner-side**: circuit construction, a one-row `__m` aggregate view, and a
one-row sentinel for the empty-`b` case (§5). No change to the compiler, VM,
relay, or accumulator — every one of those primitives was checked against the
shape this plan emits and already supports it (§3, §4).

## 1. Key insight: single-range existence is a scalar threshold

The reflex is: "does `a` match any `b`?" is a global question over `a.pk` (a left
row's matches are spread across all workers by the broadcast + per-worker trace
slice), so you must **gather** every worker's partial match set onto the `a.pk`
owner before deciding the 0/1 bit — a second exchange in series with the pair-PK
output exchange. That is true for a *general* match predicate. It is **not** true
here.

A pure-range join's `ON` clause is a **single inequality** `a.x OP b.y` — the whole
scope: `n_eq == 0`, exactly one range conjunct. The "at most one range conjunct"
rule is enforced in `collect_join_predicates` (`planner.rs:1975-1978`), the
recursive helper inside `extract_join_predicates` (run at `planner.rs:1077`); its
error propagates **before** `build_range_join_view` is reached (`planner.rs:1084`),
so ≥2 range conjuncts provably never enter the builder (test:
`test_range_join_reject_two_range_conjuncts`, `planner_join.rs:537`).
Both sides are **bare columns** — `resolve_join_col_ref` rejects any expression
(`planner.rs:1990-2006`) — so the range key is a single column, never `a.x+1` or
`b.y+b.z`. For a single inequality on bare columns, existence collapses to a
comparison against **one global scalar**:

```
∃b. a.x < b.y   ⟺   a.x < MAX(b.y)
```

`a` matches *something* iff `a.x` is below the largest `b.y`; *which* `b` matches is
irrelevant, and if `a` matches anything it matches the extreme `b`. So the
matched/unmatched split is a threshold test whose threshold is a single aggregate
of `b`:

| `ON` predicate | matched ⟺ | threshold `m` | null-fill (unmatched) ⟺ |
|---|---|---|---|
| `a.x <  b.y` | `a.x <  m` | `MAX(b.y)` | `a.x >= m` |
| `a.x <= b.y` | `a.x <= m` | `MAX(b.y)` | `a.x >  m` |
| `a.x >  b.y` | `a.x >  m` | `MIN(b.y)` | `a.x <= m` |
| `a.x >= b.y` | `a.x >= m` | `MIN(b.y)` | `a.x <  m` |

The boundary is exact because `m` is the extremum: `a.x >= MAX(b.y)` ⟺ no `b.y`
exceeds `a.x` ⟺ no `b` satisfies `a.x < b.y`. `MAX`/`MIN` ignore NULL `b.y`
(matching 3VL match-exclusion); a NULL `a.x` never matches and is null-filled
unconditionally (§5). The same reduction is why band LEFT could be seen as a
*per-eq-group* `MAX`/`MIN` — but band is already partition-local on the eq-scatter,
so only pure range needs this.

## 2. The null-fill is a range join against a one-row aggregate

With `m` in hand, the null-fill is no longer `A − distinct(π_A(inner))`. It is
simply the left rows on the unmatched side of the threshold:

```
nullfill = null_extend( A ⋈ M  on  a.x OP' m )            // OP' = the null-fill op above
         ∪ null_extend( σ_{a.x IS NULL}(A) )              // NULL-key rows, always unmatched
   where  M = { m }   is the single-row view  SELECT MIN/MAX(b.range_col) FROM b
```

`A ⋈ M` is an ordinary range join whose **trace side is one row**. This is the
crux of the elegance:

- **No `distinct`, no set-difference, no per-`a.pk` gather.** Existence is decided
  by an inequality against `m`, computed wherever `a` already lives.
- **Weight-exact / robust.** A one-row `M` cannot multiply: each unmatched `a`
  joins `M` exactly once, so the null-fill carries `a`'s own weight verbatim. The
  `A − distinct(D)` form (foundations §2) instead clamps `D` to weight 1 while `A`
  keeps its multiplicity, **over-filling a matched preserved row of weight `m>1` by
  `m−1`** — a *reachable* defect, not a hypothetical (see callout below). The
  threshold form is correct for a **bag-semantics left input** (a view, a
  `UNION ALL`) with no extra assumption.
- **Minimal shared state.** The only cross-worker information is the scalar `m`,
  not `O(matches)` of gathered `proj_a`.
- **No within-epoch cancellation to reason about.** Matched (`a.x OP m`) and
  null-filled (`a.x OP' m`) are **disjoint by the threshold**: a matched `a` makes
  `A ⋈ M` emit nothing, so the inner pair and the null-fill never coexist for the
  same `a` in a single epoch. The `distinct`-absorbs-`ΔA`/`Δmatched`-simultaneity
  argument the set-difference form leans on (foundations §2) simply does not arise —
  there is no `+a`/`−a` boundary crossing to get right.

> **Pre-existing bug (out of scope; own plan).** The set-difference null-fill used
> by **band and INNER-range LEFT** over-fills bag-valued left inputs. It is reachable
> two ways, and the simplest needs no view at all: a base table created
> **non-`unique_pk`** retains a `(pk, payload)` row at weight 2 (`enforce_unique_pk`
> collapses duplicates only on `unique_pk` tables), and that table can be the
> preserved side directly. The view route also works — a range join's left side is
> resolved by `binder.resolve` (`planner.rs:1058`), which accepts **views** with no
> base-table guard, so a `CREATE VIEW u AS … UNION ALL …` yielding a weight-2 row is
> equally valid. Either way: `a_all = map_reindex(input_a_raw, pk_cols)`
> (`planner.rs:1561`) keeps the multiplicity (weight 2) while
> `matched = distinct(proj_a)` (`planner.rs:1556`) clamps to 1, so
> `diff = a_all − matched` (`planner.rs:1567-1568`) emits a spurious weight-1
> `(a, NULL)` for a *matched* duplicated row. The fix is an **anti-join keyed by the
> preserved-row identity** (`A ▷ distinct(matched_keys)`), which subtracts match
> *existence* while keeping `A`'s multiplicity — **not** a `distinct(a_all)`, which
> would wrongly clamp a genuinely-unmatched weight-2 row's null-fill to weight 1. It
> belongs in its own plan; this plan's threshold form is inherently free of it
> (one-row `M` cannot multiply, §2) and should ship a bag-semantics test (§7) that
> the band form fails.

The tail — `null_extend → nf_rekey → nf_proj` (the `[a.pk, A, NULL B]` build, the
`(a.pk, 0)` pair-PK re-key, the `combined_coldef` B-nullability,
`planner.rs:1569-1590`) — is reused. Two things differ from band: the **source**
(the threshold range join replaces `A − distinct(π_A(inner))`), and the **NULL-key
sub-branch** needs its own routing (pure range has no eq-scatter to place a NULL
key on one worker — §3, §5).

## 3. Construction: an auxiliary aggregate view + a single-exchange LEFT view

Two views (plus a one-row sentinel table, §5). The split is what keeps `v_left`
single-exchange.

**`__m` (auxiliary, one row).** `SELECT MIN/MAX(b.range_col) FROM b` computed in
the promoted compare type `Tc` (= `range.tc`, `planner.rs:1403`) — a no-GROUP-BY
scalar aggregate built directly with `cb.reduce_multi(input, &[], &[(MIN/MAX, col)])`
(`circuit.rs:446`; empty group cols → synthetic single-group U128 PK,
`compiler.rs:954-965`). It compiles to the existing scalar-reduce path with **its
own internal exchange** (the gather to one worker), emitting one global row.
`MIN`/`MAX` over a single numeric agg + empty group key is AggValueIndex-eligible
(`compiler.rs:1723-1728`), so a deleted extreme is replaced in `O(log N)`
(`agg.rs:399-430`), not a rescan. Its input is `b.range_col` **unioned with a
one-row sentinel** so it is never empty (§5).

**`v_left` (the user view).** A range-join view with **three** delta sources — `a`,
`b`, and `__m` — and **one** output `ExchangeShard` (pair-PK). Its circuit is the
INNER pure-range circuit (two `DeltaTraceRange` terms) plus the null-fill against
`__m` (two more `DeltaTraceRange` terms) and a dedicated NULL-key branch — **four**
`DeltaTraceRange` nodes total, all carrying `n_eq == 0`:

```
sources (all broadcast by the n_eq==0 relay): ΔA, ΔB, Δ__m
    reindex_a (a.x) → partition_filter(a.x) → trace_a       // inner + matched null-fill share these
    reindex_b (b.y) → partition_filter(b.y) → trace_b
    reindex_m (m)   →   (NO partition_filter)  → trace_m     // 1 row, replicated on every worker

inner    = ΔA⋈trace_b ∪ ΔB⋈trace_a   on range.op                          // carries a.pk,b.pk
nf_match = ( ΔA_keep⋈trace_m ∪ Δm⋈trace_a  on OP' ) → π a-cols            // non-null a.x; m payload dropped
nf_null  = σ_{a.x IS NULL}(input_a_raw) → reindex(a.pk) → partition_filter(a.pk) → π a-cols
nullfill = null_extend( nf_match ∪ nf_null ) → nf_rekey (a.pk,0) → nf_proj
out      = union(inner, nullfill) → ExchangeShard(pair-PK) → sink
```

`ΔA_keep = partition_filter(reindex_a)` and `trace_a`/`trace_b` are the unchanged
INNER pure-range builds (`planner.rs:1449-1453`). The four range nodes share
`trace_a`; each `DeltaTraceRange` is emitted per-node with its own trace register
and `rel` (`compiler.rs:1446-1470`), and the VM executes each with those fields
(`vm.rs:1002-1014`), so the inner op (`range.op`) and the null-fill op (`OP'`, its
complement) coexist — only `n_eq` must agree across nodes, not the op.
**One coupling to note:** the view-level
discriminator `circuit_range_join_n_eq` (`compiler.rs:577-582`, wrapped by
`DagEngine::view_range_join_n_eq` at `dag.rs:828`) `find_map`s a *single*
`DeltaTraceRange.n_eq` over `loaded.nodes.values()` — i.e. over **`HashMap`
iteration order**, so the node it reads is not "term 0" but an *arbitrary* one. It
is correct here only because all four nodes carry `n_eq == 0`; its own doc comment
(`compiler.rs:563-576`) already bakes in the "both bilinear terms carry the same
`n_eq`" assumption. Add a comment at the construction site so a future mixed-`n_eq`
view does not silently — and nondeterministically — miscompile.

**Distribution (no duplication — the part to get right).** `m`'s one row is on
every worker, so a *broadcast* `ΔA ⋈ trace_m` would emit every unmatched `a` on
every worker (`W` copies that the pair-PK shard would then sum to weight `W`). The
fix costs no new operator: the matched null-fill probes the **same `a.x`-partitioned
slice the inner join already owns**. `ΔA_keep` (filtered by `a.x`) and `trace_a`
(also by `a.x`) place each non-NULL `a` on exactly one worker — its `a.x` owner. So:

- `ΔA_keep ⋈ trace_m` — a newly-inserted `a` is tested against `m` on its `a.x`
  owner, once.
- `Δm ⋈ trace_a` — when `m` moves (the `b` extremum changed), the ordered range
  walk over this worker's `a.x` slice of `trace_a` re-tests the `a`s that flipped.

NULL-`a.x` rows are **not** in `reindex_a`/`trace_a` (filtered before reindex,
`planner.rs:1433`), so the `nf_null` branch taps the broadcast `input_a_raw`
(`planner.rs:1421`) and routes each by its **left PK**: `reindex(a.pk) →
partition_filter(a.pk)` keeps each NULL row on its single `a.pk` owner.
`partition_filter` keys off whatever PK the preceding reindex set (`op_partition_filter`,
`exchange.rs:91-121`) and is a **local** node (opcode 33, *not* an `ExchangeShard`;
`wire/circuit.rs:230-234`), so it adds no exchange — the view stays single-output-
exchange exactly as INNER pure-range already runs two `partition_filter`s plus one
shard through the `len()==1` compile arm (`compiler.rs:2304`).

Each null-fill row is thus produced once (on its `a.x` or `a.pk` owner) and routed
once by the pair-PK shard to its `(a.pk,0)` owner — **identical to how inner pairs
flow.** No gather, no second in-view exchange, no `W×` anything.

**Cost of an `m` move.** `m` changes only when the global extremum moves — for
random `b` that is the number of running maxima, `O(log |B|)`, so the `Δm ⋈ trace_a`
churn is usually negligible; a monotonically-increasing `b` makes `m` move every
insert. Each move emits `Δm = −old + new`, and the bilinear range walk processes
the whole tail (`a.x >= old` for the `−old` leg, `a.x >= new` for `+new`), which
consolidation nets down to the `[old, new)` flip. That wasted scan is **same order
as the inner join's own output**, which is intrinsically `O(|A|·|B|)` for a range
join (each `a` pairs with every `b` past the threshold) — so it is a constant-factor
overhead on an already-quadratic operator, not a new asymptotic cost.

**Reused, zero engine change:** the broadcast relay (`op_relay_broadcast`,
`exchange.rs:824`, selected on `range_n_eq == Some(0)` at `master.rs:820-825`),
`partition_filter`, the `DeltaTraceRange` range probe and its bilinear 2-term form,
`op_union`, `op_null_extend`, the `nf_rekey`/`nf_proj` tail, the single-`ExchangeShard`
compile arm (`compiler.rs:2332`), and the `view_range_join_n_eq`-driven dag arm
(`dag.rs:1271-1310`, which broadcasts every source of an `n_eq==0` view per-source
and runs input-relay → pre → output-relay → post). A **view** source (`__m`) is
broadcast exactly like a table source: dependencies carry no table/view distinction
(`get_dep_map`), `__m`'s reindex marks it `is_join` so its relay broadcasts
(`master.rs:796-807`), and the accumulator is keyed `(view_id, source_id)` per
worker (`reactor/exchange.rs`), handling three sources with no hard-coded count.
**New:** only planner circuit construction (the two null-fill range terms, the
`nf_null` branch, the `__m` source with no `partition_filter` on `trace_m`), the
one-row `__m` view, and the sentinel (§5).

## 4. Why no sequential-exchange capability is needed

The "two exchanges in series" blocker dissolves because the existence gather is
**factored into `__m`'s own single exchange**, which runs in a *separate view*.
`v_left` reads `__m`'s materialized one-row output as just another broadcast delta
source; within `v_left` there is a single (output) `ExchangeShard`, so the existing
`compile_view` 1-exchange arm compiles it unchanged. (The `nf_null` `partition_filter`
is local, not an exchange — §3.)

`v_left` depends on `b` twice — directly (inner term) and transitively
(`b → __m → null-fill term`) — a **diamond**. That is the established shared-source
pattern (foundations §3, "`t ⋈ view-over-t`"): one `b` push reaches the two paths in
two separate epochs (`evaluate_dag` queues work per `(view, source_id)` edge), each
rebuilt against fresh trace cursors, and the net is correct. Worked example — insert
the `b` that is the new `MAX` and gives a previously-unmatched `a` (so `a.x >= old`)
its first match (so `a.x < new = b.y`):

```
v_left ΔB epoch:  ΔB=+b ⋈ trace_a                 → +(a, b)             (inner pair; a.x < b.y)
__m:              MAX rises old→new=b.y            → Δm = −old +new
v_left Δm epoch:  Δm ⋈ trace_a  on (a.x >= m):
                    −old (w=−1): a.x >= old ✓ (a was unmatched) → −(a, NULL)   (retracts a's null-fill)
                    +new (w=+1): a.x >= new ?  a.x < new ⇒ no    → nothing      (a stays un-null-filled)
                  net on a:  −(a, NULL)
v_left net:  +(a,b), −(a,NULL)   ✓
```

The final state is always correct. Like any gnitz diamond, intermediate cascade
states may transiently hold both `(a,b)` and `(a,NULL)` before they net; this is
the existing multi-view semantics, observable only if reads are served mid-cascade
(they are snapshotted at tick boundaries), and it costs at most a write-then-retract
of a tombstone. A single-view form (band, or the rejected Approach A) avoids the
transient by absorbing both deltas in one `distinct` epoch — the price of that is
the engine surgery in §8, which this approach declines.

## 5. Edge cases and implementation items

- **Empty (or all-NULL-range) `b` ⇒ `M` must still emit a row.** A LEFT join over
  an empty right side must null-fill *every* left row. But a scalar reduce over a
  zero-row input emits **nothing** (`ops/reduce/op_reduce.rs:170-174`,
  `ops/reduce/op_gather.rs:24-26` return empty — the same gap `SELECT MAX(x) FROM ∅`
  has). The **all-NULL-range `b`** case collapses to the *same* state, not a second
  gap: `MIN/MAX` skips NULL inputs at the null-gate (`agg.rs:141-145`, pinned by
  `min_ignores_null_values`, `tests.rs:3754`), so a `b` whose `range_col` is entirely
  NULL leaves the accumulator with `has_value == false` — identical to empty, and
  emission is gated on that same flag (`op_reduce.rs:573`, `emit.rs:65-71`). So with
  empty *or* all-NULL `b`, `__m` emits no row, `trace_m` is empty, `ΔA_keep ⋈ trace_m`
  produces nothing, and *all* left rows are **dropped** instead of null-filled.
  Deleting the last `b` (or nulling its last live `range_col`) drives the same
  failure: `__m` retracts its row, the matched branch can no longer fire, and the
  inner pairs are retracted with no null-fill to replace them. `M` must be
  **non-empty at all times**, reading as the `Tc` extreme when `b` contributes
  nothing (`MAX(∅) → Tc_min` so `a.x >= Tc_min` always; `MIN(∅) → Tc_max`).

  *There is no constant/`VALUES`/literal relation source to union in:* every source
  `OpNode` is `ScanDelta`/`ScanTrace` over a real table (`wire/circuit.rs:199-235`),
  and `VALUES` exists only in `INSERT` DML (`dml.rs:383-408`). Two viable routes:
  - **Internal one-row sentinel table (no engine-core change; recommended first).**
    Create a hidden one-row table `__sent` holding the `Tc` extreme; define
    `__m = MIN/MAX( union( reindex_Tc(b.range_col), reindex_Tc(__sent.val) ) )`.
    `union` of two table sources + `reduce` are existing ops; `__sent` is static
    after its single INSERT, seeds `__m`'s trace, and is always in the global
    extreme (transparent when `b` is non-empty, since `Tc_min ≤` every `b.y`).
    Because the sentinel is a **non-NULL** value, the accumulator's `has_value` is
    always true, so this route covers empty `b` *and* all-NULL-range `b` with one
    mechanism. Lifecycle mirrors `__m` (internal, dropped with `v_left`); one
    `__sent` per view, or a shared system one-row-per-`Tc` table to avoid per-view
    table proliferation.
  - **Identity-on-empty scalar reduce (small engine change).** Teach the
    synthetic-single-group reduce to emit the MIN/MAX identity (`Tc` extreme) when
    the accumulator has **no value** — keyed on `has_value == false` (`op_reduce.rs:573`,
    `emit.rs:65-71`), which is the *same* condition for zero-row input and all-NULL
    input, so it also covers the all-NULL case. Cleaner circuit, no per-view table,
    but touches `op_reduce`/`op_gather` and must stay internal (user-facing `MAX(∅)`
    is SQL-NULL, not the identity).

  **The sentinel/identity is the extreme of the promoted type `Tc`, not of
  `b.range_col`'s type.** If `a.x`'s domain extends below `b.range_col`'s (e.g.
  `a.x:I64`, `b.range_col:U8`), a `b`-type-min sentinel promoted to `Tc` is *not* ≤
  every `a.x`, so low `a.x` rows would be wrongly excluded on empty `b`. Aggregate
  in `Tc` (promote `b.range_col` first; `MIN/MAX` commute with the order-preserving
  promotion) and seed with `Tc`'s extreme. **Pin with a test that empties `b` and
  asserts every `a` null-fills (including `a.x` below `b.range_col`'s min), then
  re-fills `b` and asserts they retract.**

- **NULL `a.x`.** A NULL key never matches, so it is null-filled unconditionally,
  but the threshold compare `a.x OP' m` is UNKNOWN for NULL `a.x` and never emits
  it — and NULL keys are filtered out of `reindex_a`/`ΔA_keep`/`trace_a` before
  reindex (`multi_null_filter_prog`, `planner.rs:1433`). They need the dedicated
  `nf_null` branch off the **unfiltered, broadcast** `input_a_raw` (`planner.rs:1421`),
  routed once by `a.pk` (§3). Emitting them straight off `input_a_raw` on every
  worker would yield **W copies** (broadcast) that the pair-PK shard sums to weight
  W; the `reindex(a.pk) → partition_filter(a.pk)` deduplicates to the single `a.pk`
  owner. (Band gets this for free — its eq-scatter routes a NULL eq-key by an
  all-zero key to one worker; pure range has no eq-scatter, hence the explicit
  `a.pk` `partition_filter`.) **Do not** write the null branch as
  `σ_{a.x IS NULL}(ΔA_keep)`: `ΔA_keep` has NULL keys already filtered out, and it
  carries the `a.x` partition rather than the `a.pk` routing these rows need.

- **Scalar aggregate as an internal view.** The engine reduce supports an empty
  group set; the SQL surface cannot express a bare scalar aggregate — a non-grouped
  `MAX(col)` binds to `BoundExpr::AggCall` and is rejected when compiled as a
  computed projection (`"aggregate function not allowed in expression context"`,
  `expr.rs:180-184`; `planner.rs:679-688` only dispatches on GROUP-BY presence). But
  `__m` is emitted as a circuit directly via `cb.reduce_multi(input, &[], …)`
  (`circuit.rs:446`), so the SQL-surface limitation does not bind.

- **Type promotion — the `U64`-as-`I64` OPK trap is real and unguarded.** `range_col`
  is a fixed-width **integer** — STRING and FLOAT range pairs are rejected
  (`test_workers.py:1833-1834`), so there is no float/NaN/`±inf` sentinel edge and
  `MIN/MAX` is always AVI-backed. A 16-byte promoted `Tc` (`I128`/`U128`) is rejected
  up front (§9) — the reduce aggregates only ≤8-byte integer keys — so the `Tc`
  reaching `__m` is always a ≤8-byte int. Compute and compare `m` in `Tc` (`range.tc`). The
  trap: `MIN/MAX` output is type-labeled **`I64`** for *any* non-float input by the
  aggregate-output contract (`compiler.rs:884-896`), even for a `U64` `range_col` —
  the *value* is stored bit-exactly (the accumulator's `combine` is `U64`-aware,
  `agg.rs`), but the *label lies about signedness*. The OPK encoder keys its sign-flip
  on that label (`is_signed_int`, `pk.rs:24-38`), so a high-bit-set extreme
  (`b.y > I64::MAX`) encoded as `I64` would sign-flip to the **bottom** of the order
  instead of the top — a wrong threshold. The existing inner band/range join dodges
  this because `validate_range_join_key_pair` coerces both `a.x` and `b.y` to one
  common `Tc` and re-encodes them consistently; **`__m` is a new path** — `m` arrives
  from a reduce wearing the `I64` label, with no key-pair partner to coerce it.
  Concretely: the stored bits already *are* the `Tc` value (the `combine` kept them
  exact), so re-label `__m`'s single output column as `Tc` before `reindex_m` — a
  reinterpret, not a numeric `I64→Tc` convert (which would mangle a high-bit-set
  value) — so the OPK encode lands in `Tc`'s true (here unsigned) order. Nothing in
  the engine asserts this today. **Pin the `U64` boundary (`b.y > I64::MAX`)
  end-to-end.**

- **`m`'s payload is dropped.** `ΔA_keep ⋈ trace_m` outputs `[a-cols, m-cols]`;
  project to `[a-cols]` before `null_extend`, so the sentinel/aggregate row PK never
  reaches the output. `nf_null` projects to the same `[a-cols]` so the two branches
  union before `null_extend`/`nf_rekey`/`nf_proj`.

## 6. Planner change

Replace the `n_eq == 0` rejection branch (`planner.rs:1371-1375`; also refresh the
`build_range_join_view` doc comment at `planner.rs:1320-1337`, whose final line
(1337) currently says pure-range LEFT is rejected). In `build_range_join_view` (or a
thin pure-range-LEFT sibling), for `is_left_join && n_eq == 0`:

1. Emit the one-row sentinel table `__sent` (the `Tc` extreme per §5) and the
   auxiliary `__m` view (`MIN`/`MAX` of `Tc`-promoted `b.range_col` unioned with
   `__sent`, per the §1 table), both named internally and depended on by `v_left`.
2. Add `__m` as a third tagged delta source; `reindex_m` on the aggregate column
   with **no** `partition_filter`.
3. Build the null-fill from `ΔA_keep ⋈ trace_m` + `Δm ⋈ trace_a` on the null-fill op
   `OP'` (the complement of `range.op`: `< ↔ >=`, `<= ↔ >`). Mirror the inner's
   converse split (`rel_ab = converse_rel(range.op)`, `rel_ba = range.op`,
   `planner.rs:1414-1417`): the `ΔA_keep ⋈ trace_m` term (delta `a.x`, trace `m`)
   carries `converse_rel(OP')`, the `Δm ⋈ trace_a` term (delta `m`, trace `a.x`)
   carries `OP'`. Add the `nf_null` branch (`σ_{a.x IS NULL}(input_a_raw) →
   reindex(a.pk) → partition_filter(a.pk)`), project all to `[a-cols]`, then the
   **existing** `null_extend` / `nf_rekey` / `nf_proj` tail (`planner.rs:1569-1590`),
   unioned into the inner before the single pair-PK `shard` (`planner.rs:1599`).
   Comment the uniform-`n_eq` assumption of `circuit_range_join_n_eq` (§3).

The INNER skeleton, the broadcast/`partition_filter` split (`planner.rs:1449-1453`),
the pair-PK re-key, and the output shard are unchanged.

## 7. Testing (once built)

- **Cross-worker unmatched.** Broadcast ΔA over a scattered `b`; a left row whose
  `x < b.y` holds for no `b` on any worker → exactly one `(a,NULL)` (no per-worker
  duplicate — the `a.x`-owner produces it once). A row matching `b`s on several
  workers → pairs, no null-fill.
- **`m`-flip across workers.** Delete the extreme `b` (the one defining `m`): every
  `a` between the old and new `m` flips to null-filled though those `a`s were never
  touched. Delete a *non-extreme* `b`: `m` unchanged, **no** spurious null-fill (the
  proof that existence depends only on the extremum).
- **Same-epoch insert-and-match.** Insert `a` whose only matching `b` is already in
  trace (so `m` already covers it) → one pair, and `a.x < m` ⇒ `A ⋈ M` emits nothing
  → no transient `(a,NULL)` in the final state.
- **Empty `b` and all-NULL-range `b`.** Empty `b` → all `a` null-fill (sentinel `m`),
  **including an `a.x` below `b.range_col`'s type min** (the `Tc`-extreme regression,
  §5); re-insert `b` → the now-matched `a`s retract. Separately, a `b` with rows but
  **all `range_col` NULL** must behave identically (all `a` null-fill) — no existing
  test covers MAX/MIN over an all-NULL group (`min_ignores_null_values`,
  `tests.rs:3754`, only mixes one NULL), so add it; then null the last non-NULL
  `b.range_col` and assert the matched `a`s re-null-fill.
- **NULL `a.x` across workers.** A NULL-`a.x` row null-fills regardless of `m`, and
  exactly **once** under broadcast (no `W×` duplicate — the `a.pk` `partition_filter`
  regression).
- **All four ops** (`< <= > >=`) with `MAX`/`MIN` thresholds and exact boundary
  (`a.x == m`). Include an unsigned (`U64`) `range_col` to exercise the `I64`-output
  ordering trap (§5).
- **Bag-semantics left input.** A weight-2 left row — simplest via a non-`unique_pk`
  base table, or a `UNION ALL` view (§2) — with: matched → zero null-fill,
  unmatched → weight-2 null-fill. This is the robustness win the set-difference form
  fails (§2); assert it directly (and add the mirror as a *failing* `xfail` on the
  band path to pin that pre-existing bug until its own plan lands).
- **Mirror the band-LEFT E2E cases** (`crates/gnitz-py/tests/test_workers.py`,
  class `TestRangeJoin`: `test_band_left_delta_a_then_b`:1865,
  `test_band_left_same_epoch_match`:1917, `test_band_left_delete_matched_row`:1941,
  `test_band_left_cross_worker_eq_groups`:1961, `test_band_left_null_join_key`:1988,
  `test_band_left_payload_fidelity_wide_pk`:2014) to a pure-range predicate.

The reject test `test_pure_range_left_join_rejected` (`planner_join.rs:521` — asserts
`Unsupported` containing `"pure-range LEFT JOIN"` *and* no view registered) flips
to accept (`.unwrap()` + `.is_ok()`) plus a circuit-shape assertion; the E2E reject
entry `"pure_range_left"` (`test_workers.py:1831`) in `test_sql_rejections` (fn at
`:1816`) moves to a positive test. The shape assertion is **not** a verbatim mirror
of `test_band_left_join_circuit_shape` (`planner_join.rs:643`, which asserts the
band LEFT view adds `distinct +1`, `negate +1`, `union +2`, `null_extend +1`,
`ExchangeShard +0` over INNER): the threshold form has **zero** `distinct` and
**zero** `negate` (no negate-driven set-difference), and instead **+2
`DeltaTraceRange`** terms vs the INNER pure-range view (four total) plus one
`null_extend` and the plain term/branch-combine `union`s, with exactly one
`ExchangeShard`. `opcode_node_count`
(`tests/common/mod.rs:143`) counts opcodes per `view_id`, so the extra `__m` source
cannot be asserted as a `v_left` opcode — assert `__m` separately (resolve it as its
own aggregate view, or count `v_left`'s `ScanDelta` sources). The merge-walk vs
per-row equivalence is pinned by the **Rust** unit test
`test_range_dt_merge_vs_per_row_differential` (`join.rs:1792`, helper at `:1767`).

## 8. Alternatives considered and rejected

Both were earlier designs; the §1 threshold makes them unnecessary. Kept for the
reviewer who hits a case the threshold does **not** cover — a *general* match
predicate (multiple range conjuncts, a non-monotonic `ON`, an arbitrary UDF) where
existence genuinely is an opaque per-`a.pk` question. The pure-range scope is not
such a case.

- **A — in-view sequential (multi-stage) exchange.** Keep the band null-fill circuit
  but insert an `a.pk` `ExchangeShard` between `proj_a` and `distinct`, so the view
  runs `pre → shard₁(a.pk) → mid → shard₂(pair-PK) → post`. Needs deep engine
  surgery the threshold approach avoids: a third `CompileOutput` phase and a new
  sequential carve in `compile_view` (whose 2-exchange arm assumes *parallel*,
  disjoint ancestor sets — it partitions nodes by `ancestors_inclusive` of each
  exchange input, `compiler.rs:2402-2426`; two *sequential* exchanges put one in the
  other's ancestor set and miscompile, today guarded only by the planner reject); a
  multi-output PRE VM entry (`proj_a`+`projected`+`a_keep` share the join);
  per-boundary shard-col resolution (`get_shard_cols`/`get_exchange_info` `find_map`
  an *arbitrary* one of two `ExchangeShard`s — `dag.rs:676`,`:743` — which also
  breaks the existing pair-PK output relay); a third relay round whose discriminant
  must avoid `0` (the reserved output-relay `source_id`, `master.rs:796`,
  `dag.rs:1308`) and every input `source_id` (which are **table ids**: system 1–15
  or user ≥ `FIRST_USER_TABLE_ID = 16`, `wire/catalog.rs:68`) — a raw circuit
  node-id (which start at 1, `core/circuit.rs:226`) would alias the system table ids,
  so use a negative/reserved discriminant; and `prepare_relay` extended from its two
  kinds (broadcast vs scatter, `master.rs:822-854`) to three. It also gathers the
  full `proj_a` (`O(matches)`) every tick and inherits the multiplicity gap (§2).
  Strictly heavier on every axis here.
- **B — `a.pk`-prefix view partitioning.** Partition `v_left` by the `a.pk` prefix so
  one `a.pk` exchange co-locates each left row with all its matches and its null-fill.
  Breaks the pervasive "partition key == `view_pk`" invariant (`partition_for_pk_bytes`
  at every scatter/scan/seek) and — decisively — **replicates the B trace `W×`**:
  each `a.pk` owner must probe all of `b`, so `trace_b` can no longer be
  `partition_filter`ed to ~1/W. The threshold approach keeps the INNER 1/W trace
  profile and adds only a one-row `trace_m`.

## 9. Scope

- Only pure-range (`n_eq == 0`, single inequality, bare integer columns) LEFT. Band
  LEFT (`n_eq ≥ 1`) and pure/band INNER are implemented in `build_range_join_view`.
- The compare type `Tc` must be a **≤8-byte integer**. A 16-byte promoted `Tc`
  (`I128`/`U128` — e.g. `U64` vs `I64` → `I128`, or a `U128`/`UUID` pair) is rejected
  up front, because the `MIN`/`MAX` reduce backing `__m` aggregates only ≤8-byte
  integer keys (the same limit the binder enforces on user-facing `MIN`/`MAX`). The
  reject lands before any `__m`/sentinel objects are created.
- RIGHT / FULL OUTER range joins stay rejected (as for equi).
- `__m` and the sentinel table are internal dependencies of `v_left`; they are not
  user-visible and are dropped with the view.
- The band/INNER-range bag-multiplicity over-fill (§2 callout) is a separate,
  pre-existing bug with its own fix; it is not addressed here.
