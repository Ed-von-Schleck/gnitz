# LEFT OUTER band join (equality-prefixed range)

## Goal

`CREATE VIEW … AS SELECT … FROM a LEFT JOIN b ON <eq…> AND <range>` is rejected
today — the `is_left_join` guard inside the range arm of `execute_create_join_view`
(`planner.rs:1083-1087`, *"LEFT JOIN with a range predicate is not supported"*).
Only INNER range / band joins exist. This plan adds the LEFT variant **for band
joins (≥1 equality conjunct, `n_eq ≥ 1`)**: every left row with ≥1 range match
emits the matched pairs (already produced by the INNER circuit); every left row
with **no** match emits one null-filled row (`[a cols…, NULL b cols]`,
weight = `w_a`), maintained incrementally under both ΔA and ΔB.

The whole feature is a planner change inside `build_range_join_view`
(`planner.rs:1335`) plus the one-line rejection swap — **no new engine operator
and no new builder method**. The null-fill is a pure **Z-set difference**: the
left input minus the matched left rows, where the matched left rows (*with their
full payload*) are read straight off the INNER join's own output. The only
non-linear node is one `distinct`; everything else is `map`, `map_reindex`,
`union`, `negate`, `null_extend` — all linear.

The entire null-fill computation is **partition-local**: a band join's eq-prefix
input scatter co-locates each left row with all of its candidate matches on one
worker (§3), so existence is decided with no extra exchange, and the null-fills
ride the **same** pair-PK output exchange as the inner pairs. The view stays
pair-PK-partitioned exactly like the INNER range view.

```sql
-- band LEFT (n_eq ≥ 1): a-rows with no b in their k-group satisfying the range → (a, NULL)
CREATE VIEW v AS SELECT a.id, b.id FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t;
```

**Pure-range LEFT (`n_eq == 0`, no equality conjunct) is rejected** — it needs a
global per-`a.pk` existence gather *in series* with the pair-PK output exchange,
i.e. two sequential exchanges, which the single-exchange-boundary executor cannot
run (§3, §6). It is its own plan: `plans/range-join-left-outer-pure-range.md`.

## 1. The matched set is keyed by left identity, not by key

The equi LEFT join (`execute_create_join_view`, `planner.rs:1214-1242`) tracks
matches as `distinct(B_keys)`, keyed by the **join key**: for an equi-join "`a` has
a match" ⟺ "`a`'s join key ∈ B's key set", a function of **B alone**.

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
disjoint epochs" simplification is gone; §2 absorbs the simultaneity in `distinct`.

## 2. The null-fill is a set difference, and `distinct` absorbs the simultaneity

The null-fill is the anti-join of the left input against the matched set:

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
multiplicity (rows 7–8) is handled because `distinct` tracks net weight per `a.pk`
and only emits on the 0-boundary crossing.

## 3. Distribution — the null-fill is partition-local (band only)

The INNER band circuit (`n_eq ≥ 1`) eq-prefix-scatters both inputs on the input
relay (`master.rs:811-826`): a left row and every `b` that *could* match it share
the eq-key, so they land on the **same worker**. The range probe is therefore
partition-local (the band INNER join integrates the scattered reindex directly,
with **no** `PartitionFilter` — `planner.rs:1427-1431`), and so is the whole
null-fill:

- A given `a.pk`'s row sits on exactly one worker (its eq-key's). Every one of
  `a`'s matches (`b`s with that eq-key) sits on that same worker. So
  `{ all inner rows for a.pk } ∪ { the left row for a.pk } ⊆` one worker. The
  existence test `distinct(π_a(inner))` over `a.pk` is then **globally correct
  computed locally** — no `a.pk` exchange. (`a.pk` is unique, so each `a.pk`'s
  rows never span workers; this is the same property that makes the band probe
  partition-local.)
- `A`: every left row re-keyed to `a.pk` is *already* on its eq-key worker from
  the scatter, so a plain `map_reindex` re-keys it in place — **no shard**. It is
  co-located with `D`, so `union(A, −D)` runs locally.

Only the existing pair-PK **output** exchange remains. The null-fills are re-keyed
to the pair-PK and unioned with the inner pairs *before* that one `shard`, so they
ride it together; the view is pair-PK-partitioned exactly as the INNER range view
(§6).

This single-exchange shape is the only one the executor supports. The compiler
splits a circuit at its `ExchangeShard` nodes and accepts **0, 1, or 2** of them
(`compiler.rs:2282-2512`); the 2-node case is strictly two *parallel* set-op sides
(carved by disjoint `ancestors_inclusive` sets, combined in the post phase), and
`_ => Err(-6)`. A null-fill that exchanged by `a.pk` and *then* by the pair-PK
would be two **sequential** exchanges — unsupported. Band avoids the `a.pk`
exchange entirely; pure range (`n_eq == 0`) cannot (§6).

`π_a` weights sum correctly per `a.pk` *before* `distinct`: a left row matching `b1`
and `b2` accumulates weight 2 at `a.pk`, a retraction of one match drops it to 1
(still "present"), and only when the last match retracts does `distinct` cross to 0.
The pair-PK byte-identity the inner output needs for its own ±1 cancellation is
irrelevant here — projecting to the A columns drops `b`, so an AB-emitted `+a` and a
later BA-retracted `−a` for the same left row collapse to the same `(a.pk, A)`
element and net in `distinct` regardless of which `b` carried them.

## 4. Output schema, null-fill PK, NULL keys

**Pair-PK with a sentinel `b.pk`.** The view PK is the source-PK pair
`[a.pk…, b.pk…]` (`planner.rs:1462-1464`, `:1509`). A null-fill row has no `b`, so
re-keying it onto the pair-PK reads the (NULL) `b.pk` columns from the
null-extended B region, which `map_reindex`'s NULL-key rule packs to the synthetic
`0`. The null-fill's PK is thus `(a.pk, 0…)` with the `b` payload columns marked
NULL in the bitmap. It never `(PK,payload)`-merges with a real `(a.pk, b.pk=0…)`
inner row because consolidation compares the full row and `compare_rows` orders
NULL strictly below any value (`null < non-null`,
`columnar.rs:test_compare_rows_null_lt_non_null`), so the NULL-`b` null-fill and a
real `b`-payload row are distinct elements. `a.pk` is unique in A, so distinct left
rows get distinct null-fill PKs.

**B columns become nullable in the catalog.** A `LEFT` join can emit a physical
NULL for any B column, so the view schema must mark every B column `is_nullable`
or a client decoding a NULL in a `NOT NULL` column panics. The equi path already
does this (`planner.rs:1276-1280`); the range path's `combined_coldef` must match
(§5).

**Byte-exact cancellation is load-bearing (§8).** A matched left row's `+1` (from
`A`) and `−1` (from `D`) must be byte-identical in `(PK, payload)` so they
consolidate to zero. §5 guarantees this by routing *both* `A` and the inner
A-projection through `map_reindex` with the **same self-derive `a.pk` target types
and the same verbatim A-column copy**, so the `(a.pk, A)` bytes are identical by
construction (the only difference is the input addressing, not the output
encoding).

**NULL join keys need no special case.** A left row with a NULL in any eq or range
column can never satisfy the predicate (SQL 3VL), so the inner join's existing NULL
filter (`planner.rs:1410-1412`) keeps it out of the inner output and therefore out
of `D`. But it is still a left row, so it is in `A` (which taps the *unfiltered*
input, §6) → `A − D` null-fills it automatically. `a.pk` is non-nullable (PK
columns always are), so re-keying `A` by `a.pk` is always well-defined even when a
join-key column is NULL; the NULL value rides along as an ordinary (NULL-marked) A
payload column. **No `input_a_match` / `input_a_null` split, no bypass union** —
the difference subsumes it.

## 5. The circuit, by builder calls

Replace the re-key/project/shard/sink section of `build_range_join_view`
(`planner.rs:1466-1507`, up to `let circuit = cb.build();`) with the block below;
the trailing `view_pk` / `debug_assert` / `reject_duplicate_column_names` /
`create_view_with_circuit` (`planner.rs:1509-1519`) is unchanged (`pair_pk_idxs`
stays in scope for its `debug_assert`). The INNER segment up to `projected` is
unchanged except for `combined_coldef`'s nullability fix; the LEFT null-fill is
added between `projected` and the final `shard`. `merged` keeps its
`[_join_pk × k, A, B]` layout (PK region = the `_join_pk` range key, payload =
`[A cols, B cols]`); `input_a_raw` is the left input *before* the inner NULL filter
(§6).

```rust
    let merged = cb.union(proj_ab_node, proj_ba_node);

    // ---- union schema + pair-PK re-key (INNER output; unchanged) ----
    let mut union_cols: Vec<ColumnDef> = Vec::with_capacity(k + left_n + right_n);
    for (i, &t) in all_tcs.iter().enumerate() {
        union_cols.push(ColumnDef {
            name: format!("_join_pk_{i}"), type_code: TypeCode::from_validated_u8(t),
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        });
    }
    for col in &left_schema.columns  { union_cols.push(col.clone()); }
    for col in &right_schema.columns { union_cols.push(col.clone()); }
    let union_schema = Schema { columns: union_cols, pk_cols: (0..k).collect() };

    let mut pair_pk_cols: Vec<usize> = Vec::with_capacity(pair_pk);
    for &a_pk in &left_schema.pk_cols  { pair_pk_cols.push(k + a_pk); }
    for &b_pk in &right_schema.pk_cols { pair_pk_cols.push(k + left_n + b_pk); }
    let zero_tcs = vec![0u8; pair_pk];
    let rekey = cb.map_reindex(merged, &pair_pk_cols, &zero_tcs, build_reindex_program(&union_schema));

    let payload_offset = pair_pk + k;
    // LEFT join: every B column can be NULL in a null-fill row → mark it nullable.
    let combined_coldef = |idx: usize| -> ColumnDef {
        if idx < left_n {
            left_schema.columns[idx].clone()
        } else {
            let mut c = right_schema.columns[idx - left_n].clone();
            if is_left_join { c.is_nullable = true; }
            c
        }
    };

    let pair_pk_coldefs: Vec<ColumnDef> = left_schema.pk_cols.iter().map(|&c| (left_schema, c))
        .chain(right_schema.pk_cols.iter().map(|&c| (right_schema, c)))
        .enumerate()
        .map(|(slot, (schema, c))| ColumnDef {
            name: format!("_pair_pk_{slot}"),
            type_code: schema.columns[c].type_code.reindex_output_type(),
            is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
        })
        .collect();

    let is_wildcard = select.projection.iter().all(|p| matches!(p, SelectItem::Wildcard(_)));
    let (final_cols, final_projection) = build_join_view_projection(
        &select.projection, alias_map, &pair_pk_coldefs, left_n + right_n, payload_offset,
        combined_coldef, "range JOIN view",
    )?;

    let projected = cb.map(rekey, &final_projection);

    // ---- LEFT null-fill (band only; partition-local — see §3) ----
    let sink_input = if !is_left_join {
        projected
    } else {
        let zero_a = vec![0u8; pa];

        // D = distinct(π_A(inner)) in a.pk space. A reindex Map's OUTPUT schema is
        // always [new-PK, <every input column>] (`reindex_output_schema`), regardless
        // of what the copy program writes — so a "copy A only" program would still
        // declare B and the _join_pk slots in the layout. The only clean [a.pk, A] is
        // re-key (copy the whole union payload, mirroring the inner rekey) THEN project
        // the A region. A's PK cols sit at `k + a_pk` in `merged`.
        let a_pk_in_merged: Vec<usize> =
            left_schema.pk_cols.iter().map(|&c| k + c).collect();
        let rekey_a = cb.map_reindex(
            merged, &a_pk_in_merged, &zero_a, build_reindex_program(&union_schema));
        // rekey_a payload = [_join_pk × k, A, B]; A starts at absolute col `pa + k`.
        let a_cols: Vec<usize> = (pa + k..pa + k + left_n).collect();
        let proj_a = cb.map(rekey_a, &a_cols);          // [a.pk, A]
        let matched = cb.distinct(proj_a);              // local distinct (§3)

        // A = every left row (incl. NULL-key rows the inner match filtered out),
        // re-keyed to a.pk with the IDENTICAL [a.pk, A] encoding as proj_a so a matched
        // row's +1 (A) and −1 (D) are byte-identical and cancel (§4, §8).
        let a_all = cb.map_reindex(
            input_a_raw, &left_schema.pk_cols, &zero_a, build_reindex_program(left_schema));

        // A − D, then attach the NULL B columns: [a.pk, A, NULL B].
        let right_col_tcs: Vec<u64> =
            right_schema.columns.iter().map(|c| c.type_code as u64).collect();
        let neg = cb.negate(matched);
        let diff = cb.union(a_all, neg);
        let nullfill = cb.null_extend(diff, &right_col_tcs);

        // Re-key nullfill ([a.pk×pa (PK), A, B]) onto the pair-PK. a.pk from the PK
        // region (0..pa); b.pk from the NULL B payload (pa + left_n + b_pk) → packs to
        // the synthetic 0 → null-fill PK = (a.pk, 0…). The reindex output schema is
        // fixed to [pair-PK, a.pk×pa, A, B]; copy ONLY the A and B columns at their
        // schema positions (src = dst = pa + ci) and leave the a.pk×pa payload slots
        // unwritten (0) — they are projected away by nf_proj.
        let mut nf_pair_pk_cols: Vec<usize> = (0..pa).collect();
        for &b_pk in &right_schema.pk_cols { nf_pair_pk_cols.push(pa + left_n + b_pk); }
        let mut eb = ExprBuilder::new();
        for ci in 0..left_n + right_n {
            let tc = if ci < left_n {
                left_schema.columns[ci].type_code as u32
            } else {
                right_schema.columns[ci - left_n].type_code as u32
            };
            eb.copy_col(tc, (pa + ci) as u32, (pa + ci) as u32);
        }
        let nf_rekey = cb.map_reindex(nullfill, &nf_pair_pk_cols, &zero_tcs, eb.build(0));
        // nf_rekey payload = [a.pk×pa (0), A, B]; user cols sit `pa` past where the
        // inner rekey ([_join_pk × k, …]) puts them. Same user-column SET as the inner
        // (shift each inner payload index by pa − k), so both branches agree on final_cols.
        let nf_projection: Vec<usize> =
            final_projection.iter().map(|&idx| idx - k + pa).collect();
        let nf_proj = cb.map(nf_rekey, &nf_projection);

        cb.union(projected, nf_proj)
    };

    let pair_pk_idxs: Vec<usize> = (0..pair_pk).collect();
    let sharded = cb.shard(sink_input, &pair_pk_idxs);
    cb.sink(sharded);
    let circuit = cb.build();
```

Index derivations (all mechanical, no new helper):

- `rekey_a` reuses `build_reindex_program(&union_schema)` (the inner rekey's
  program) and `union_schema` — copies the whole `[_join_pk × k, A, B]` payload;
  `proj_a` then selects the A region (`pa + k .. pa + k + left_n`). Re-keying by
  `a_pk_in_merged` with `zero_a` (self-derive) and copying A verbatim makes
  `proj_a`'s `(a.pk, A)` bytes equal `a_all`'s for matched rows: same a.pk source
  types → same self-derived `reindex_output_type` → same OPK bytes; A copied
  verbatim → same payload bytes. This symmetry is the cancellation invariant
  (§4, §8).
- `a_all` re-keys the unfiltered left input by `left.pk_cols` with `zero_a`; its
  output schema `[a.pk, A]` matches `proj_a`'s exactly.
- `nf_rekey`'s custom program copies A at output payload offsets `pa..pa+left_n`
  and B at `pa+left_n..` (their positions in the fixed `[pair-PK, a.pk×pa, A, B]`
  schema), so `nf_proj`'s offset is `pair_pk + pa`, i.e. `idx − k + pa` over the
  inner `final_projection` (whose offset is `pair_pk + k`). The user-column set is
  identical, so `final_cols` describes both branches.
- `right_col_tcs` is the same all-B-columns null-extend type list the equi path
  builds (`planner.rs:1136-1137`).

No scheduling constraints: the null-fill core is a straight chain
(`distinct → negate → union → null_extend → map_reindex → map`); the only fan-outs
are `merged` (→ inner `rekey` + `rekey_a`) and `input_a_raw` (→ inner NULL-filter +
`a_all`), both read by non-destructive `map_reindex`/`filter`. The
destructive-register ordering rule (`planner.rs` Kahn tie-break) does not bite: the
one destructive node (`distinct`, which empties its input register) is the sole
reader of `proj_a`.

## 6. Planner wiring

- **Swap the rejection for the LEFT path** (`planner.rs:1083-1093`): drop the
  `if is_left_join { return Err(…) }` guard and pass `is_left_join` into
  `build_range_join_view`:

  ```rust
      if let Some(range) = range_conjunct {
          return build_range_join_view(
              client, schema_name, view_name, sql_text, select, &alias_map,
              left_tid, right_tid, &left_schema, &right_schema,
              &left_join_cols, &right_join_cols, &target_tcs, range,
              is_left_join,
          );
      }
  ```

- **Thread `is_left_join: bool`** as the last parameter of
  `build_range_join_view` (`planner.rs:1335-1350`). The INNER path is
  byte-identical when `false`.

- **Reject pure-range LEFT** right after `let n_eq = left_join_cols.len();`
  (`planner.rs:1353`):

  ```rust
      // Pure-range LEFT (no equality conjunct) needs a global per-a.pk existence
      // gather in series with the pair-PK output exchange — two sequential exchanges,
      // which the single-boundary executor cannot run (compiler.rs:2282). Band LEFT
      // (n_eq ≥ 1) co-locates each left row with all its candidate matches on the
      // eq-prefix worker, so the null-fill is partition-local (§3).
      // See plans/range-join-left-outer-pure-range.md.
      if is_left_join && n_eq == 0 {
          return Err(GnitzSqlError::Unsupported(
              "pure-range LEFT JOIN (no equality conjunct) is not supported; add an \
               equality conjunct (band join) or use INNER JOIN".into()));
      }
  ```

- **Keep a handle to the unfiltered left input.** Rename the
  `input_delta_tagged(left_tid)` result to `input_a_raw` and feed the NULL filter
  from it (`planner.rs:1403`, `:1410-1412`):

  ```rust
      let input_a_raw = cb.input_delta_tagged(left_tid);
      // …
      let input_a = if left_key_nullable {
          cb.filter(input_a_raw, Some(multi_null_filter_prog(&left_reindex_cols, left_schema, false)?))
      } else { input_a_raw };
  ```

  The inner match uses `input_a` (NULL keys dropped); `a_all` taps `input_a_raw`
  so NULL-key rows are null-filled (§4). `input_b` keeps its plain non-null filter
  (an unmatched B row never appears in the inner output, hence never in `D`).

- **`final_cols` / `view_pk` are unchanged** from the INNER range join: same
  output schema, `pair_pk` columns, and `ExchangeShard` on the pair-PK; LEFT only
  adds rows (the null-fills) into that same shape.

- **RIGHT / FULL OUTER stay rejected** for range exactly as for equi — only INNER
  and LEFT are matched at `planner.rs:1047-1055`; the `_ => Unsupported` arm is
  untouched.

## 7. Engine: no new operator, no new builder

Every node the null-fill uses already exists and is heavily exercised:
`map_reindex`, `distinct` / `op_distinct`, `negate`, `union`, `null_extend` /
`op_null_extend`, `map`. There are **no joins** in the null-fill path — no
anti-join, no delta-trace join, no delta-delta join — and **no extra trace
tables**: the only persistent state is `distinct`'s own integral. The INNER range
op `op_join_delta_trace_range` and its `Join(DeltaTraceRange { n_eq, rel })` node
are untouched — the LEFT join reuses the inner output, it does not extend the range
probe. The `AntiJoin`/`SemiJoin` `DeltaTraceRange` wire variants stay
`unreachable!`.

Consequences of the set-difference formulation:

- **Cheaper per epoch.** No merge-walk joins in the null-fill path; `distinct` is
  O(|delta|) point lookups, the rest is linear scans.
- **Less state.** One `distinct` trace (`a.pk` + A payload) — no re-integrated copy
  of the matched set, no full A-integral.
- **No subtle timing.** The only non-linearity is inside `distinct`; there is no
  current-vs-delayed trace question and no destructive-register scheduling to get
  right.

**foundations.md update (at implementation time).** foundations.md states the LEFT
outer join only as a per-delta-row null-fill (`:176-185`). Add the model-level
identity this plan uses (algebra only): a LEFT outer join is
`inner ∪ null_extend(A − distinct(π_A(inner)))`, where `π_A(inner)` carries the
preserved side's full payload, so the null-fill is a Z-set **difference**, not an
anti-join requiring payload recovery. The matched set is keyed by the **preserved
row's identity** (its source PK); for an equi join it may instead be keyed by the
join key as the B-only `distinct(B_keys)` optimization, but the identity-keyed
difference is the general form and the only one available when matches span an
interval. Because the lone non-linear operator (`distinct`) absorbs the
within-epoch ΔA/Δ-matched simultaneity, no delta-delta cross term is needed despite
the matched set depending on the preserved side.

## 8. Correctness invariants to preserve

- **Byte-exact `(A, D)` cancellation.** A matched left row must be byte-identical in
  `A` (`a_all`) and `D` (`proj_a` → `distinct`) so `union(A, −D)` consolidates it to
  zero (and stays zero through `null_extend` → re-key → projection). Guaranteed by
  routing both through `map_reindex` with identical self-derive `a.pk` targets
  (`zero_a`) and verbatim A-column copies, producing the same `[a.pk, A]` schema
  (§5). An E2E test with string / nullable / wide A payloads pins it (§9) — a
  stride/encoding mismatch surfaces as a spurious leftover `(a, NULL)`.
- **Matched-set existence, not multiplicity.** `D = distinct(π_A(inner))` collapses
  per-`a.pk` match multiplicity to a 0/1 bit; an over-counted or tombstoned inner
  pair must not change `a`'s null-fill until the net count crosses 0 — guaranteed by
  `distinct` over the `a.pk`-summed weights (§3).
- **Partition-local distinct.** The `distinct` runs in the **pre-exchange** phase
  with no preceding `shard`; it is correct only because the eq-prefix scatter
  co-locates every row sharing an `a.pk` on one worker (§3). It survives
  `opt_distinct` (its input `proj_a` is not `is_distinct_at`: the upstream reindex
  `rekey_a` breaks the distinctness chain, `compiler.rs:614-628`). The
  multi-worker E2E (§9) is the guard.
- **Null-fill identity is `(a.pk, 0…)` + NULL-b bitmap.** `nf_rekey` packs the NULL
  `b.pk` columns to 0 and leaves the `b` payload NULL, so a null-fill never merges
  with a real `b.pk = 0` row (`compare_rows`: NULL < any value). Do not route
  null-fills through the inner `rekey`/`final_projection` (whose payload offset
  includes the `k` `_join_pk` slots the null-fills lack) — use `nf_rekey` /
  `nf_projection` at offset `pair_pk + pa`.
- **Schema agreement.** `nf_projection` is `final_projection` shifted by `pa − k`,
  so both branches select the identical user-column set and both shard on the same
  `pair_pk_idxs`; the pre-shard `union(projected, nf_proj)` therefore merges one
  stride.
- **`D ⊆ A`.** Every inner row's left side is a (non-NULL-key) left row, so
  `A − D ≥ 0` everywhere. Relies on `a_all` being sourced from the *same* left input
  that feeds the inner join (`input_a_raw` ⊇ the NULL-filtered inner input).
- **Self-join stays rejected** independent of LEFT/range (`planner.rs:1065-1069`).

## 9. Testing

**Engine unit** — none; no operator or builder changes (the reused ops keep their
existing tests: `op_distinct`, `op_null_extend`, `op_union`, `op_map`/reindex).

**Circuit shape (`gnitz-sql` planner tests).** A band LEFT range view, over the
INNER band skeleton, adds exactly: `map_reindex ×3` (`rekey_a`, `a_all`,
`nf_rekey`), `map ×2` (`proj_a`, `nf_proj`), `distinct ×1`, `negate ×1`,
`union ×2` (the `A − D` diff and the inner∪null-fill combine), `null_extend ×1`.
Assert it adds **zero `ExchangeShard`, zero `Join`/`AntiJoin`/`SemiJoin`, zero
`IntegrateTrace`/`Delay`/`PartitionFilter`** beyond the inner skeleton; exactly
one `distinct`, fed by the `proj_a` `map` (NOT a `shard`); `a_all` is a
`map_reindex` of the *unfiltered* left input. INNER range views are byte-identical
to before (`is_left_join == false`). A pure-range LEFT view
(`a LEFT JOIN b ON a.x < b.y`) returns `Unsupported`.

**Multi-worker E2E (`gnitz-py/tests/test_workers.py::TestRangeJoin`,
`GNITZ_WORKERS=4`).** A Python cross-filter reference computes the expected LEFT
result (matched pairs ∪ `(a, NULL)` for unmatched a). All cases are band
(`n_eq ≥ 1`):

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
- **Cross-worker eq-groups.** Seed `a` rows in several *distinct* eq-key groups so
  the scatter spreads them across workers, some groups matched and some not → each
  worker's local null-fill is correct and the global view is the exact LEFT result
  (the partition-local `distinct` must not emit a spurious null-fill for a matched
  `a`, nor drop one for an unmatched `a` whose eq-group lives on another worker).
- **Byte-identity / payload fidelity.** Use a left table with a STRING column, a
  nullable column carrying NULL, and (if available) a wide PK; insert matched rows
  and assert the null-fill set is empty (clean `A − D` cancellation) and the matched
  pairs carry the exact A payload. Pins the §8 byte-exact-cancellation invariant.
- **NULL join key.** A left row with NULL eq or NULL range column → exactly one
  `(a, NULL)`, never matched, never in `D` (no bypass branch exists).
- **Retraction / multiplicity.** Delete one of two `b` matches for an `a` → `a` stays
  matched (no null-fill); delete the last → `(a, NULL)` appears.

## 10. Scope boundaries (by design, not deferred)

- **Pure-range LEFT joins (`n_eq == 0`)** are rejected — the null-fill would need a
  per-`a.pk` existence gather in series with the pair-PK output exchange (two
  sequential exchanges, unsupported). Own plan:
  `plans/range-join-left-outer-pure-range.md`.
- **RIGHT / FULL OUTER range joins** are rejected, matching the equi join's
  INNER+LEFT-only support (`planner.rs:1047-1055`).
- **Multiple range conjuncts / residual ON predicates** remain rejected (one
  eq-prefix + one range conjunct only, as for INNER); a residual predicate under a
  LEFT join would null-fill on the residual too, which the post-join `Filter` plan
  must handle (`wide-pk-incremental-views.md` §1).
- **No range anti/semi/outer operator.** The outer semantics are a set difference
  over `distinct(π_a(inner))`, reusing the existing inner output and linear ops; the
  range probe is reused, not extended.
