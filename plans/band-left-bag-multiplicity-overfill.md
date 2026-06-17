# Range/band LEFT join: one weight-exact threshold null-fill for all `n_eq ≥ 0`

## Goal

Replace the band LEFT join's null-fill — currently the Z-set difference
`A − distinct(π_A(inner))` (`planner.rs:1720-1744`) — with the **per-eq-group
threshold-subtraction form**, the same construction the pure-range path
(`n_eq == 0`, `planner.rs:1616-1719`) already uses, generalized to `n_eq ≥ 1`.
The result is a **single** range/band LEFT null-fill builder parameterized by
`n_eq`, replacing the `if n_eq == 0 { … } else { … }` split.

Two payoffs:

1. **Weight-exactness.** The set-difference form clamps matched multiplicity to 1
   via `distinct`, so a matched weight-`w` preserved row leaks a spurious
   weight-`(w−1)` `(left, NULL)` (foundations §3, "`distinct` clamps
   multiplicity"). The threshold form carries each `a`'s true weight (a
   one-row-per-group witness cannot multiply), so it is weight-exact for a
   bag-valued left input too. This bug is **not SQL-reachable** today (§2) — the
   value is parity and defense-in-depth, not live data correction.
2. **Distributional simplicity.** Band scatters `b` and `a` by the eq prefix, so
   each eq-group's `b`, its threshold `M_eq`, and the `a`s sharing that prefix are
   co-located on one worker. The per-group reduce is a normal partition-local
   GROUP BY (1× state) — band is the *easier* distributional case than pure
   range, which broadcasts and recomputes one global `m` on every worker.

**Dependency.** This builds on `plans/adaptive-minmax-agg-output-type.md`: the
threshold reduce must emit `MAX/MIN(b.range_col)` typed as the range slot type
`range.tc` so `reindex_m` self-derives the matching OPK order. With adaptive
MIN/MAX that holds for **any ≤8-byte integer** range column; the range-column
requirement is then ≤8-byte int for **both** band and pure-range LEFT (the
pure-range cap relaxes from `{I64, U64}`). 16-byte range columns (U128/UUID/I128)
have no 8-byte MIN/MAX accumulator and are rejected — consistent with the binder,
which already rejects MIN/MAX on those types (`binder.rs:308-319`).

## 1. The defect in the set-difference form

`planner.rs:1720-1744`, the `n_eq ≥ 1` branch:

```rust
let proj_a  = cb.map(rekey_a, &a_cols);     // [a.pk, A], matched left rows read off inner
let matched = cb.distinct(proj_a);          // [a.pk, A], CLAMPED to ±1
let a_all   = cb.map_reindex(input_a_raw, &left_schema.pk_cols, &zero_a, ...); // [a.pk, A], weight w
let neg = cb.negate(matched);
cb.union(a_all, neg)                        // a_all − matched
```

A matched preserved row of weight `w`: `a_all = +w·a`, `distinct(matched) = +1·a`,
so `nf = (w−1)·a` — should be `0`. The clamp is in the subtraction: `distinct`
caps matched multiplicity at 1 while `a_all` keeps `w`. Exact only when the
preserved side has unique identities (weight ≤ 1).

## 2. Reachability: LATENT (not SQL-reachable)

No SQL surface delivers a weight-≥2 (same PK, same payload) row to a range/band
join's left input. Every multiplicity path collapses to weight-1, and
content-colliding paths re-key to distinct PKs:

- **Base tables are always `unique_pk`.** `execute_create_table` calls
  `client.create_table(.., &pk_indices, true, ..)` (`planner.rs:501`).
  `enforce_unique_pk` collapses same-PK accumulation; the DML validator rejects
  `w > 1` on a unique-PK table. The non-`unique_pk` flavor exists only on the raw
  `gnitz-core`/`gnitz-py` client.
- **`UNION ALL` views branch-tag.** `compile_set_op_side` keys each side via
  `map_hash_row(.., branch_id)` (`planner.rs:2946`); `(Union, All)` sets the
  right side's `branch_id = 1` (`planner.rs:3056-3062`), so two identical payloads
  hash to **distinct** PKs → two weight-1 rows, never one weight-2 row.
- **Projection/GROUP BY/DISTINCT/join views** are PK-keyed or clamped:
  `map_reindex` keys by listed columns (never a content hash), GROUP BY emits one
  row/group, DISTINCT is clamped, joins are PK-pair keyed.

So the bug triggers **only** via the low-level client
(`create_table(unique_pk=false)` + a weighted batch `push`) plus a `CREATE VIEW …
range/band LEFT JOIN` over it. Severity: weight-exactness parity / defense-in-depth.

## 3. The naive anti-join fix is WRONG (record this)

The tempting fix — mirror equi LEFT with an anti-join keyed by `[eq, range]`,
`matched_keys = distinct(map_key_only(merged))`,
`nf = anti_join(reindex_a, I(matched_keys))` plus a
`negate(join(matched_keys, trace_a))` correction — **is incorrect**. Band-match
existence is **not** a function of `B` alone (it depends on the left row's own
range value), so `matched_keys` is derived from the inner output and is entangled
with `ΔA`. A left row inserted in the **same epoch** it matches an *existing* `b`:

- `anti_join(reindex_a, z⁻¹(I(matched_keys)))` tests the new row against the
  **old** matched-keys trace, which lacks its key → **spurious null-fill**;
- the `negate(join(Δmatched_keys, z⁻¹(trace_a)))` correction joins the new key
  against the **old** `A` trace, which has no row with that key → emits nothing.

Net: `+(a, b)` (inner) **and** `+(a, NULL)` (spurious), never retracted. Equi LEFT
avoids this only because `distinct(B keys)` is `ΔB`-driven and stable in a `ΔA`
epoch. Band's witness must likewise be **right-side only** — which the threshold
form provides (`M_eq` is a `ΔB`-driven reduce, integrated so `int_a ⋈ trace_M`
sees the current `M_eq`).

The set-difference form is, by contrast, simultaneity-*correct*
(`distinct(proj_a)` is a delta op that sees the new match in-epoch, netting
`+a − a = 0`); its only defect is the weight clamp. The threshold form keeps
simultaneity-correctness **and** adds weight-exactness.

## 4. The fix — per-eq-group threshold for all `n_eq ≥ 0`

Match existence collapses to a per-eq-group threshold: `a` matches ⟺
`a.range OP M_{a.eq}`, where `M_{eq} = MAX(b.range)` (for `<`/`<=`) or `MIN`
(for `>`/`>=`) over `b` in eq-group `eq`. The null-fill is the subtraction form
(foundations §3, empty-other-side robust, weight-exact via the one-row-per-group
witness):

```
M_eq      = reduce_multi_local(reindex_b, group=[eq prefix], [(MAX/MIN, range)])  // one row per eq-group
trace_M   = integrate_trace(reindex_M)                                             // keyed [eq, range], partition-local
matched   = (int_a ⋈ trace_M) ∪ (Δm ⋈ trace_a)   on range.op, carrying n_eq       // weight-exact
nf_match  = int_a − matched                                                        // union(int_a_keyed, negate(matched))
nf_null   = NULL-key branch (left rows with a NULL eq/range column)
out       = union(inner, null_extend(nf_match ∪ nf_null) → rekey pair-PK) → shard → sink
```

For `n_eq == 0` (pure range) the group set is empty → one global `m`, the eq
prefix is absent (`k = 1`), and the input is broadcast; for `n_eq ≥ 1` the group
is the eq prefix, `k = n_eq + 1`, and the input is eq-scattered. Both are the same
construction parameterized by `n_eq`.

### 4.1 Early requirement: ≤8-byte integer range column

The threshold reduce aggregates `b.range_col` with MIN/MAX (an 8-byte
accumulator) and `reindex_m` reindexes the result onto the range slot type
`range.tc`. With `plans/adaptive-minmax-agg-output-type.md` the reduce emits the
result typed `range.tc` for any ≤8-byte integer, so the requirement is exactly
"≤8-byte integer range column" — for **both** `n_eq == 0` and `n_eq ≥ 1`. Replace
the `n_eq == 0`-only cap at `planner.rs:1483-1492` with:

```rust
// Range/band LEFT: the threshold null-fill reduces the range column with MIN/MAX
// into an 8-byte accumulator, then reindexes the result onto the range slot type.
// A ≤8-byte integer range column is required (narrow widths are supported via
// adaptive MIN/MAX output typing); a 16-byte U128/UUID/I128 range column has no
// 8-byte accumulator. Reject up front rather than failing the compile.
if is_left_join && !gnitz_wire::is_fixed_int(range.tc) {
    let range_tc = TypeCode::from_validated_u8(range.tc);
    return Err(GnitzSqlError::Unsupported(format!(
        "range/band LEFT JOIN needs a ≤8-byte integer range column (got {range_tc:?}); \
         its threshold null-fill reduces the range column with MIN/MAX, which has no \
         16-byte accumulator — use a narrower range column or INNER JOIN")));
}
```

`is_fixed_int` is the U8..U64 / I8..I64 set (`gnitz-wire/src/types.rs`); it
excludes U128/UUID/I128 (16-byte) and floats/strings (already rejected by
`validate_range_join_key_pair`).

### 4.2 The unified null-fill builder (`n_eq ≥ 0`)

Replaces the entire `let nf_keyed = if n_eq == 0 { … } else { … };` block
(`planner.rs:1616-1745`). `int_a`, `trace_a`, `reindex_b`, `k`, `pa`, `left_n`,
`zero_a`, `rel_ab`, `rel_ba`, `all_tcs`, `left_reindex_cols`, `left_key_nullable`
are as already bound earlier in `build_range_join_view`.

```rust
let nf_keyed = {
    let range_tc = TypeCode::from_validated_u8(range.tc);
    let want_max = matches!(range.op, RangeRel::Lt | RangeRel::Le);
    let agg_func = if want_max { AGG_MAX } else { AGG_MIN };

    // (1) Move reindex_b's k key slots (eq prefix + range) into payload, decoding
    //     OPK → native and carrying each slot's type. The reduce then aggregates
    //     the native range value (not OPK bytes, which would invert signed
    //     MIN/MAX). mbh = [hash_pk:U128, eq_0…eq_{n_eq-1}, range:Tc].
    let mbh = cb.map_hash_row(reindex_b, &(0..k).collect::<Vec<_>>(), 0);

    // (2) Per-eq-group MIN/MAX threshold, shard-free: reindex_b is already
    //     eq-scattered (band) or broadcast (pure range), so a local reduce yields
    //     the right per-group / global extremum with no second exchange. Group by
    //     mbh's eq cols [1..=n_eq] (empty for pure range); aggregate the range col
    //     at 1+n_eq. With adaptive MIN/MAX the agg column self-types `range.tc`.
    let group_cols: Vec<usize> = (1..=n_eq).collect();
    let red = cb.reduce_multi_local(mbh, &group_cols, &[(agg_func, 1 + n_eq)]);

    // (3) red's schema MUST mirror the engine's build_reduce_output_schema(mbh,
    //     [1..=n_eq], MIN/MAX): mbh's PK is the hash (col 0), so group_set_eq_pk is
    //     never true; use_natural_pk holds iff is_single_col_natural_pk — n_eq == 1
    //     and mbh's lone eq col (non-nullable, since it is a projected reindex PK
    //     slot) is U64/U128/UUID. Natural: [eq_0:Teq, m:Tc]. Synthetic:
    //     [_group_pk:U128, eq_0…eq_{n_eq-1}, m:Tc]. Every column non-nullable
    //     (PK slots are non-nullable; MIN/MAX m is non-null because reindex_b is
    //     non-empty per group when a row exists).
    let eq_tc = TypeCode::from_validated_u8(all_tcs[0]);
    let natural_pk = n_eq == 1
        && matches!(eq_tc, TypeCode::U64 | TypeCode::U128 | TypeCode::UUID);
    let nonnull = |name: &str, tc: TypeCode| ColumnDef {
        name: name.into(), type_code: tc, is_nullable: false, fk_table_id: 0, fk_col_idx: 0 };
    let red_cols: Vec<ColumnDef> = if natural_pk {
        vec![nonnull("_jpk", eq_tc), nonnull("m", range_tc)]
    } else {
        let mut c = vec![nonnull("_group_pk", TypeCode::U128)];
        for i in 0..n_eq { c.push(nonnull(&format!("_jpk_{i}"), TypeCode::from_validated_u8(all_tcs[i]))); }
        c.push(nonnull("m", range_tc));
        c
    };
    let red_schema = Schema { columns: red_cols, pk_cols: vec![0] };

    // (4) Reindex red onto [eq prefix, range] (k slots). red already carries the
    //     promoted slot types, so self-derive (target tcs all 0). Natural: eq col
    //     at red 0, m at 1. Synthetic: eq cols at 1..=n_eq, m at 1+n_eq.
    let reindex_cols_m: Vec<usize> = if natural_pk {
        vec![0, 1]
    } else {
        (1..=n_eq).chain(std::iter::once(1 + n_eq)).collect()
    };
    let target_tcs_m = vec![0u8; k];
    let reindex_m = cb.map_reindex(red, &reindex_cols_m, &target_tcs_m, build_reindex_program(&red_schema));
    let trace_m   = cb.integrate_trace(reindex_m);

    // (5) The two matched terms carry n_eq (eq-pin then range-compare), mirroring
    //     join_ab/join_ba exactly with the trace swapped to trace_m / reindex_m.
    //     All range nodes in the view then agree on n_eq, which the view-level
    //     circuit_range_join_n_eq discriminator (reads one arbitrary node) needs.
    let j_am = cb.join_with_trace_range_node(int_a, trace_m, n_eq as u8, rel_ab);
    let j_ma = cb.join_with_trace_range_node(reindex_m, trace_a, n_eq as u8, rel_ba);
    // Range-join output = [_join_pk × k, delta payload, trace payload]. j_am: A is
    // the delta payload at k..k+left_n. j_ma: A is the trace payload, after Δm's
    // full payload (reindex_m's payload = all of red's columns).
    let m_payload = red_schema.columns.len();
    let m_am = cb.map(j_am, &(k..k + left_n).collect::<Vec<_>>());
    let m_ma = cb.map(j_ma, &(k + m_payload..k + m_payload + left_n).collect::<Vec<_>>());
    let matched_raw = cb.union(m_am, m_ma);                 // [_join_pk × k (PK), A]

    // (6) Re-key matched_raw and the bare int_a passthrough onto [a.pk…, A] with
    //     the IDENTICAL encoding (one shared reindex program), so a matched a's +a
    //     (passthrough) and −a (matched, negated) are byte-identical and cancel
    //     in-epoch. Both inputs are [k key slots (PK), A]; nf_raw_schema carries k
    //     PK slots (not 1) — the join PK arity is k = n_eq + 1.
    let mut nf_raw_cols: Vec<ColumnDef> = (0..k).map(|i| ColumnDef {
        name: format!("_jp_{i}"), type_code: TypeCode::from_validated_u8(all_tcs[i]),
        is_nullable: false, fk_table_id: 0, fk_col_idx: 0,
    }).collect();
    nf_raw_cols.extend(left_schema.columns.iter().cloned());
    let nf_raw_schema = Schema { columns: nf_raw_cols, pk_cols: (0..k).collect() };
    let a_pk_in_raw: Vec<usize> = left_schema.pk_cols.iter().map(|&p| k + p).collect();
    let a_cols: Vec<usize> = (pa + k..pa + k + left_n).collect();
    let nf_reindex_prog = build_reindex_program(&nf_raw_schema);
    let rekey_a = |cb: &mut CircuitBuilder, input: gnitz_core::NodeId| {
        let keyed = cb.map_reindex(input, &a_pk_in_raw, &zero_a, nf_reindex_prog.clone());
        cb.map(keyed, &a_cols)                              // [a.pk…, A]
    };
    let matched = rekey_a(&mut cb, matched_raw);
    let a_pass  = rekey_a(&mut cb, int_a);
    let neg = cb.negate(matched);
    let nf_match = cb.union(a_pass, neg);                   // A − matched, keyed [a.pk…, A]

    // (7) NULL-key branch: NULL eq/range rows are filtered out of int_a/trace_a
    //     (3VL) and never match the threshold, so re-add them off the UNFILTERED
    //     input_a_raw, re-keyed to a.pk. Pure range (n_eq == 0) broadcasts a, so
    //     dedup the W copies with a local partition_filter. A band join (n_eq ≥ 1)
    //     eq-scatters a, delivering each NULL-eq row to exactly ONE worker, so
    //     partition_filter must be OMITTED — it filters by a.pk partition and would
    //     drop every NULL-eq row whose a.pk partition ≠ its eq-scatter worker
    //     (data loss). The pair-PK output exchange re-homes the single copy.
    if left_key_nullable {
        let anull = cb.filter(input_a_raw,
            Some(multi_null_filter_prog(&left_reindex_cols, left_schema, true)?));
        let anull_keyed = cb.map_reindex(
            anull, &left_schema.pk_cols, &zero_a, build_reindex_program(left_schema));
        let anull_owned = if n_eq == 0 { cb.partition_filter(anull_keyed) } else { anull_keyed };
        cb.union(nf_match, anull_owned)
    } else {
        nf_match
    }
};
```

The shared tail (`null_extend → nf_rekey → nf_proj → union(projected) → shard →
sink`, `planner.rs:1747` onward) is unchanged: it keys off `nf_keyed`'s
`[a.pk…, A]` identity for both `n_eq` cases.

### 4.3 Removals

- Delete the band set-difference branch (`planner.rs:1720-1744`: `rekey_a` /
  `proj_a` / `matched` / `a_all` / `neg` / `union`, including the `cb.distinct`).
- Delete `pure_range_m_output_cols` (`planner.rs:2026-2033`); the inline
  `red_cols` construction above subsumes it.
- The equi join (`execute_create_join_view`) keeps its anti-join-by-key null-fill
  (no range conjunct; its witness `distinct(B keys)` is already right-side-only
  and weight-exact).

## 5. Correctness (weight arithmetic)

Matched weight-`w` left row `a` (eq = g, matches some `b`): `int_a = +w·a`;
`int_a ⋈_{a.range OP M_g} trace_M = +w·a` (one-row `M_g` per group cannot
multiply, so it carries `a`'s weight `w`); `nf = w·a − w·a = 0`. ✓ Unmatched
weight-`w` (`a.range` past `M_g`, or eq-group `g` empty so `M_g` absent):
`matched = 0`, `nf = +w·a`. ✓ Empty `b` in group `g` ⇒ `trace_M` has no row for
`g` ⇒ `A − ∅ = A`, every `a` in `g` null-fills (foundations §3 empty-other-side
robustness). Within-epoch insert-and-match: `M_g` is `ΔB`-stable and integrated,
so `int_a ⋈ trace_M` sees the current `M_g` and nets `a` in the same epoch (§3).
Threshold-move (the extreme `b` deleted in group `g`): `Δm ⋈ trace_a` re-tests
that group's `a`s.

## 6. Testing

- **Bag weight-exactness (the regression guard).** Not SQL-constructible (§2), so
  a `gnitz-py` raw-client test: `unique_pk=false` left table, push a weight-2 row
  that band-matches; assert null-fill weight `0` (matched) and `2` (an unmatched
  weight-2 row). Fails on the `distinct` set-difference form, passes on the fix.
- **Narrow range column (the cap-relaxation guard).** A band LEFT and a pure-range
  LEFT whose range column is `INT` (I32) / `INT UNSIGNED` (U32): both register and
  produce correct null-fills across multiple workers. These do **not** compile on
  the current pure-range cap and exercise the adaptive-MIN/MAX dependency.
- **16-byte range column rejected.** A band/pure-range LEFT on a `U128`/`UUID`
  range column returns `Unsupported` ("≤8-byte integer range column"); no view is
  registered. The matching INNER join still registers.
- **Full band LEFT suite stays green** (`test_workers.py`, `TestRangeJoin`
  `test_band_left_*`): `delta_a_then_b`, `same_epoch_match` (the simultaneity
  guard §3 — matched-on-insert yields no `(a, NULL)`), `delete_matched_row`,
  `cross_worker_eq_groups`, `null_join_key` (the NULL-key branch, now without
  `partition_filter`), `payload_fidelity_wide_pk`.
- **NULL-key multi-worker (band).** Extend `null_join_key` so left rows with a
  NULL eq column have a.pk partitions that differ from their eq-scatter worker —
  the regression guard for the §4.2-(7) `partition_filter` omission: a
  `partition_filter` here drops these rows.
- **Empty / deleted eq-group.** One eq-group's `b` empty (all its `a`s null-fill)
  and another deleted-to-empty (its `a`s re-null-fill).
- **Circuit shape.** `test_band_left_join_circuit_shape` (`planner_join.rs:749`):
  the band view loses the `distinct`/set-difference and gains the threshold shape
  — a `Reduce` grouped by the eq prefix, `+2` range terms (inner 2, null-fill 2),
  one `null_extend`, one extra `IntegrateTrace` (`trace_m`), one `negate`, the
  same two sources, exactly one `ExchangeShard`, and zero `Distinct` — converging
  on the shape `test_pure_range_left_join_circuit_shape` (`planner_join.rs:575`)
  asserts. Assert `OPCODE_DISTINCT == 0` and `OPCODE_REDUCE == 1`.

## 7. Scope

- **Range/band LEFT (`n_eq ≥ 0`).** Equi LEFT (anti-join by key) is already
  weight-exact and unchanged; INNER joins have no null-fill and are unrestricted
  (any range type, via the existing inner terms).
- **One builder.** Pure-range and band LEFT share the single threshold
  construction; the `if n_eq == 0 { … } else { … }` split and the band
  set-difference form are deleted.
- **Range-column requirement is ≤8-byte integer** for LEFT joins, band and pure
  range alike (16-byte rejected, consistent with the binder's MIN/MAX policy).
  Depends on `plans/adaptive-minmax-agg-output-type.md`.
