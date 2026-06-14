# Pure-range LEFT OUTER join (no equality conjunct)

## Goal

Support `CREATE VIEW … AS SELECT … FROM a LEFT JOIN b ON <range>` with **no**
equality conjunct (`n_eq == 0`), e.g.:

```sql
CREATE VIEW w AS SELECT a.id, b.id FROM a LEFT JOIN b ON a.x < b.y;
```

The INNER pure-range join already exists. The band LEFT join (≥1 equality
conjunct) is `plans/range-join-left-outer.md`. This is the missing case: a LEFT
range join with no eq-prefix. **It is blocked on an engine capability** (a second
exchange in series), not a planner detail — see §2. This plan characterizes the
blocker and the two ways to lift it.

## 1. The algebra is identical to the band case

The null-fill is the same Z-set difference as the band plan (read that plan's §2):

```
D = distinct( π_{A-cols}( A ⋈θ B ) )       // matched left rows, full A payload, one per a.pk
nullfill = A − D                            // union(A, negate(D)), linear; D ⊆ A
```

`distinct` absorbs the within-epoch ΔA/ΔD simultaneity; the case table and all the
node-level details (`combined_coldef` nullability, the `nf_rekey` `pa` offsets,
the reindex-then-project for `proj_a`, byte-exact `(A, D)` cancellation) are
identical to the band plan §4–§5 and §8. The **only** difference is distribution.

## 2. The blocker: pure range needs two exchanges in series

A pure range join **broadcasts** ΔA and ΔB to every worker and trims each side's
trace to its owned 1/W slice with `PartitionFilter` (`planner.rs:1427-1431`,
`master.rs:811-826`). Consequently a left row's matches are spread across **all**
workers — worker `w` only ever sees `a`'s matches against `w`'s slice of `b`. So
"does `a` have any match" is a **global** question over `a.pk`:

- The existence test `D = distinct(π_a(inner))` must **gather** every worker's
  partial match set onto the single owner of `a.pk` before `distinct` decides the
  0/1 bit. That gather is an exchange keyed by `a.pk`.
- The view, like every other join view, is partitioned by its full PK — the
  source-PK **pair** `(a.pk, b.pk)` — so the inner pairs *and* the finished
  null-fills must finally be exchanged by the **pair-PK** for the output.

`a.pk` ≠ pair-PK, and the second exchange consumes the output of the first
(`distinct` → `union` → re-key → pair-PK shard). They are **sequential**, not
parallel.

The executor does not support that. The compiler splits a circuit at its
`ExchangeShard` nodes and accepts only **0, 1, or 2** of them
(`compiler.rs:2282-2513`); the 2-node case is strictly two *parallel* set-op sides
— it carves each side as `ancestors_inclusive(exchange_input)` and requires the
two ancestor sets to be **disjoint**, combining them in a single post phase. Two
*sequential* exchanges overlap (the second's ancestors include the first), so the
carve is wrong, and 3+ exchanges hit `_ => Err(-6)`. The phase model in
`CompileOutput` (`compiler.rs:134-169`) and the runtime
(`dag.rs:1298-1340`, `reactor/exchange.rs`) only ever run one exchange round, or
two independent rounds feeding one combine.

(The band case escapes this entirely: its eq-prefix scatter co-locates every left
row with all of its candidate matches on one worker, so `D` is decided locally and
only the pair-PK output exchange remains — one exchange. See
`plans/range-join-left-outer.md` §3.)

This is why `build_range_join_view` rejects `is_left_join && n_eq == 0`
(that plan §6). Lifting the rejection requires one of the two engine capabilities
below.

## 3. Approach A (recommended): sequential (multi-stage) exchange

Add a third execution phase so a view may run `pre → shard₁ → mid → shard₂ → post`.
The view stays uniformly **full-pair-PK partitioned** — no change to view scan,
seek, co-partition annotations, or the "partition key == view_pk" invariant. The
cost is confined to the compiler split and the runtime relay loop.

### Circuit (phase-tagged)

```
PRE  (per worker, broadcast input):
    reindex_a, reindex_b; partition_filter → integrate_trace; join_ab, join_ba → merged
    proj_a   = π_A(merged) re-keyed to a.pk           // [a.pk, A]   (per-worker partial matches)
    a_keep   = partition_filter(reindex(input_a_raw → a.pk))   // [a.pk, A], this worker's a.pk slice
    projected = π_user(rekey(merged → pair-PK))        // [pair-PK, user cols]  (inner pairs)

SHARD₁ (a.pk): gather proj_a onto a.pk owners.  a_keep is already a.pk-owned
    (broadcast + partition_filter), so it crosses the boundary as the second
    co-keyed side without duplication.

MID  (on a.pk owner):
    matched  = distinct(proj_a)                        // global existence, one per a.pk
    nullfill = null_extend(union(a_keep, negate(matched)))   // [a.pk, A, NULL B]
    nf       = π_user(rekey(nullfill → pair-PK))       // [pair-PK, user cols], B NULL

SHARD₂ (pair-PK): the inner pairs (`projected`, carried through stage 1 unchanged)
    and `nf` are exchanged by the pair-PK.

POST (on pair-PK owner):
    sink(union(projected_relayed, nf_relayed))
```

`projected` (inner pairs) must travel from PRE to SHARD₂ without passing through
SHARD₁ — it is pair-PK-keyed, not a.pk-keyed. The multi-stage compiler must let a
PRE-phase value bypass the first boundary and feed the second. The cleanest model
is a per-`ExchangeShard` boundary with explicit inputs, rather than the current
"one cut point in the topo order".

### Touch-points

- **`CompileOutput` phase model** (`compiler.rs:134-169`): generalize `{pre, post,
  side_b}` to an ordered list of stages with explicit per-boundary exchange schemas
  and seed registers, or add a `mid` stage + second exchange schema.
- **`compile_view` split** (`compiler.rs:2282-2513`): when two `ExchangeShard`
  nodes are in series (`ancestors_inclusive(eb_in)` ⊇ `ea`), carve
  `pre = anc(ea_in)`, `mid = anc(eb_in) \ (anc(ea_in) ∪ {ea})`, `post = rest`,
  threading each stage's exchange schema/seed. Keep the existing disjoint-ancestors
  case for parallel set-ops. A value that feeds SHARD₂ but is in PRE (the inner
  pairs) is carried by giving SHARD₂ multiple inputs.
- **DagEngine execution** (`dag.rs:1298-1340`): run PRE → IPC exchange (a.pk) →
  MID → IPC exchange (pair-PK) → POST — two relay rounds in series per tick. The
  exchange-skip / single-worker fast path (`skip_exchange`) generalizes per stage.
- **`ExchangeAccumulator`** (`reactor/exchange.rs:1-26`): the key is
  `(view_id, source_id)`; a second in-view round collides. Add the `ExchangeShard`
  node-id (or stage index) to the key so the two rounds stay disjoint.
- **Master relay routing** (`master.rs:811-826`): the mid `a.pk` scatter is a third
  relay kind alongside input broadcast/scatter and the output pair-PK scatter; route
  it by the `a.pk` `GroupKey`.

This capability is **general**: any future view needing a re-partition between two
non-linear stages (e.g. an aggregation keyed differently from the output, or a
two-key nested incremental join) reuses it.

## 4. Approach B (alternative): a.pk-prefix view partitioning

Partition the pure-range LEFT view by the `pa` `a.pk` columns (a **prefix** of the
pair-PK) instead of the full pair-PK. Then one `a.pk` exchange co-locates each
left row, all of its matches, and all of its (single) null-fill on one worker; the
null-fill `distinct`/`union` and the inner-pair combine all run **post that one
shard**, and the view is `a.pk`-partitioned. The view PK stays the full
`(a.pk, b.pk)` for row uniqueness (an `a` matches many `b`).

### Why it is harder than it looks

- **Breaks "partition key == view_pk"**, an invariant the band plan §6 relies on
  and that is pervasive: `partition_for_pk_bytes` is called identically by the
  output scatter, the view scan, the seek, and master routing, all assuming the
  full PK. Every such site must consult a per-view *partition prefix* distinct from
  `view_pk`. This is cross-cutting and the risky part.
- **Still ≥3 boundary-crossing streams.** The post phase needs the inner pairs
  (`[pair-PK, user]`), `a_keep` (`[a.pk, A]`), and `proj_a` (`[a.pk, A]`) — three
  inputs across the boundary, where the current 2-side model allows two. Either
  extend the combine to 3 inputs, or pre-combine — but `proj_a` cannot be merged
  into `a_keep` before the shard because `distinct(proj_a)` is necessarily
  post-shard.

Approach B trades the multi-stage runtime change for a multi-stream + prefix-
partitioning change. Because prefix-partitioning perturbs the storage/scan layer
(higher blast radius than a confined compiler/relay change) and the multi-stream
problem remains, **Approach A is recommended.**

## 5. Planner change (once the capability exists)

Trivial on top of `plans/range-join-left-outer.md`: remove the `n_eq == 0`
rejection in `build_range_join_view`, and in the `is_left_join` branch emit the
phase-tagged circuit of §3 (Approach A) — `a_keep` uses `partition_filter` (not a
bare reindex) because the input is broadcast; `proj_a`, `a_keep` shard by `a.pk`
(SHARD₁); `projected` and `nf` shard by the pair-PK (SHARD₂). The null-fill node
construction (`proj_a` reindex-then-project, `nf_rekey` `pa` offsets,
`combined_coldef` nullability, `right_col_tcs`) is identical to the band plan §5.

## 6. Testing (once built)

- **Pure-range LEFT, cross-worker unmatched.** `n_eq == 0`, broadcast ΔA over a
  scattered `b`: a left row whose `x < b.y` matches nothing on *any* worker → exactly
  one `(a, NULL)` (the `a.pk` gather must see the globally-empty match set, not emit
  a spurious null-fill per worker). A left row matching `b`s on several workers → the
  pairs, no null-fill, and `(a,NULL)` retracted if it had been emitted.
- **ΔB flips existence across workers.** A left row matched only on worker `w₁`;
  delete that `b` (on `w₁`) → `(a, NULL)` appears though `a` was never touched on the
  other workers (the gather re-crosses 0).
- **Same-epoch insert-and-match, cross-worker.** Insert a left row whose only match
  sits in `b`'s trace on a *different* worker → one pair, no transient `(a, NULL)`.
- The band plan §9 byte-identity / NULL-key / multiplicity cases, run with
  `n_eq == 0`.
- **Two-stage exchange unit tests** (Approach A): a synthetic
  `pre → shard₁ → mid → shard₂ → post` circuit compiles into three stages, both IPC
  rounds key distinctly in the accumulator, and a PRE value bypassing SHARD₁ reaches
  SHARD₂.

## 7. Scope

- Only the pure-range (`n_eq == 0`) LEFT join and the engine capability it needs.
  Band LEFT (`n_eq ≥ 1`) is `plans/range-join-left-outer.md`.
- RIGHT / FULL OUTER range joins stay rejected (as for equi).
- The multi-stage exchange (Approach A) is specified only as far as this feature
  needs; a broader rollout to other view shapes is out of scope here.
