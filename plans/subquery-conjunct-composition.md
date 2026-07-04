# Multiple EXISTS/IN conjuncts + operator-over-sub-plan composition

## Problem

The hidden-view chain substrate (atomic `create_view_chain`, dependency-ordered
backfill, cascading DROP, the `emit_*_pieces` source-override pattern,
`compile_join_to_hidden`) is shipped and proven. It already composes: multi-way joins
(inner + LEFT/RIGHT/FULL), self-joins, GROUP BY / DISTINCT directly over a join, and
DISTINCT/set-op **finals** over a compiled CTE sub-plan.

Two composition gaps remain, both "an operator whose input is itself a non-linear
sub-plan." The first has **no workaround** and is the real deliverable:

1. **Multiple `[NOT] EXISTS` / `[NOT] IN` conjuncts** in one WHERE
   (`… WHERE EXISTS(A) AND EXISTS(B)`). The classifier peels exactly one subquery
   conjunct and rejects a second (`dispatch.rs`: "at most one EXISTS/IN subquery
   conjunct per view WHERE"). There is no CTE workaround: an `EXISTS`-bodied CTE is not
   a compilable hidden body (`lower_linear` cannot bind an `EXISTS` in the WHERE), so it
   cannot be pre-materialized either.

2. **Set-op directly over a raw join, per side**
   (`(SELECT … FROM a JOIN b) UNION (SELECT … FROM c)`). `resolve_side_source` rejects a
   JOIN'd FROM. The CTE form (`WITH j AS (a JOIN b) SELECT … FROM j UNION …`) works today,
   so this is low priority.

## Segment shape (load-bearing fact)

An EXISTS/IN semi/anti-join view does **not** carry the outer's natural PK. It emits

```
[ synthetic_pk, <outer projection columns…> ]
```

where `synthetic_pk` is the k `_join_pk` columns for an equi correlation (no output
exchange — re-keying to the outer PK is physically impossible) or the `_src_pk` columns
(the outer source PK under fresh names) for a band / pure-range correlation. This is the
documented shape-dependent PK surface (`exists.rs` header) and it is identical to what a
single-conjunct EXISTS view produces today.

The outer's own columns — including the base table's PK column — ride the **payload** at
their original names, resolved through an outer-only alias map. That is what makes a
later conjunct's correlation (`… WHERE inner.x = outer.y`) resolve **by name** against the
running relation. But the leading `synthetic_pk` is *not* the outer's natural PK, and a
wildcard projection re-expands over the whole segment schema (`build_join_view_projection`
iterates all `n_combined` outer columns), so a naïve `SELECT *` chain would accumulate one
`_join_pk`/`_src_pk` per link into the payload — leaking internal columns to the user view
and, on a long chain, breaching `MAX_COLUMNS`. The chain builder prevents this with
projection shielding (below); no provenance machinery is otherwise needed.

## Committed design

### 1. Multiple subquery conjuncts → a chain of semi/anti-join segments

Each `[NOT] EXISTS`/`[NOT] IN` conjunct becomes one hidden semi/anti-join segment over
the previous:

```
h0    = outer     WHERE subq0  [AND <linear locals>]
h1    = h0        WHERE subq1
...
final = h_{n-2}   WHERE subq_{n-1}
```

Every segment carries the **base-outer columns** in its payload, so `h_{i}` is a valid
outer for `h_{i+1}` and correlations resolve by name. The synthetic PK is replaced (not
accumulated) at each link by shielding the projection.

**Mechanics** (all in `gnitz-sql/src/plan/view/`):

- **Outer source-override.** Thread an `Option<(u64, Rc<Schema>)>` into
  `emit_exists_pieces`. The outer *alias* is still extracted from
  `select.from[0].relation` (the base table's alias — so correlations resolve by name);
  only the `(tid, schema)` is overridden with the prior segment. `None` keeps the
  existing base-table path.

  ```rust
  pub(crate) fn emit_exists_pieces(
      client: &mut GnitzClient,
      view_id: u64,
      select: &Select,
      subq: &Expr,
      outer_not: bool,
      local_conjuncts: &[&Expr],
      binder: &mut Binder<'_>,
      outer_override: Option<(u64, Rc<Schema>)>,   // NEW
  ) -> Result<EmitPieces, GnitzSqlError> {
      // …
      let (outer_name, outer_alias) =
          extract_table_name_and_alias(&select.from[0].relation, "CREATE VIEW EXISTS/IN")?;
      let (outer_tid, outer_schema) = match outer_override {
          Some(o) => o,
          None => binder.resolve(client, &outer_name)?,
      };
      // …unchanged: build_alias_map([(outer_alias, outer_tid, outer_schema)]) etc.
  }
  ```

  The single-conjunct call site (`dispatch.rs`) passes `None`.

- **Projection shielding.** Intermediate segments (`h0..h_{n-2}`) project **every base-outer
  column by name**: this drops the prior segment's leading synthetic PK from the payload and
  keeps *all* base columns available — a later conjunct may correlate on a column the user
  never SELECTed. The final segment (`h_{n-1}`) applies the user projection with any `*`
  pre-expanded to the base-outer columns (a raw wildcard would re-expand over the last
  segment's schema and re-admit its synthetic PK). Both cases reach the same final view
  schema shape a single-conjunct EXISTS produces: `[synthetic_pk, base cols…]`.

- **Chain builder.** A new `create_subquery_chain_view` mirrors `plan_join_chain`: emit
  `h0..h_{n-2}` onto `chain` via `ViewChain::add_segment`, threading each prior segment as
  the next's `outer_override`; emit the final with `final_vid` and return its
  `EmitPieces`. Linear locals fold into `h0` only (they filter the base outer, once).

  ```rust
  pub(crate) fn create_subquery_chain_view(
      client: &mut GnitzClient,
      binder: &mut Binder<'_>,
      final_vid: u64,
      select: &Select,
      subqs: &[(&Expr, bool)],   // (peeled node, outer_not), syntactic order
      local: &[&Expr],
      chain: &mut ViewChain,
  ) -> Result<EmitPieces, GnitzSqlError> {
      // Base outer resolved once: its columns are the payload every segment carries.
      let (base_name, _) =
          extract_table_name_and_alias(&select.from[0].relation, "CREATE VIEW EXISTS/IN chain")?;
      let (_, base_schema) = binder.resolve(client, &base_name)?;

      // Intermediates project all base-outer columns by name (drop prior synthetic
      // PK, keep every column for downstream correlations).
      let pass_through: Vec<SelectItem> = base_schema
          .columns
          .iter()
          .map(|c| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(c.name.clone()))))
          .collect();

      // Final projection: user items, with `*` pre-expanded to base-outer columns.
      let mut final_proj: Vec<SelectItem> = Vec::new();
      for item in &select.projection {
          match item {
              SelectItem::Wildcard(_) => final_proj.extend(pass_through.iter().cloned()),
              other => final_proj.push(other.clone()),
          }
      }

      let n = subqs.len();
      let mut prev: Option<(u64, Rc<Schema>)> = None;
      for (i, &(subq, outer_not)) in subqs.iter().enumerate().take(n - 1) {
          let mut seg = select.clone();
          seg.projection = pass_through.clone();
          let locals: &[&Expr] = if i == 0 { local } else { &[] };
          let prev_override = prev.clone();
          let (vid, schema) = chain.add_segment(client, |client, _chain, vid| {
              emit_exists_pieces(client, vid, &seg, subq, outer_not, locals, binder, prev_override)
          })?;
          prev = Some((vid, schema));
      }

      // Last conjunct → the user-named final view.
      let (subq, outer_not) = subqs[n - 1];
      let mut seg = select.clone();
      seg.projection = final_proj;
      let locals: &[&Expr] = if n == 1 { local } else { &[] };
      emit_exists_pieces(client, final_vid, &seg, subq, outer_not, locals, binder, prev)
  }
  ```

- **Peel all conjuncts.** `ViewShape::Subquery` (`dispatch.rs`) collects **every** top-level
  `[NOT] EXISTS`/`[NOT] IN` conjunct (via `as_subquery_conjunct`) into
  `subqs: Vec<(&Expr, bool)>` plus the remaining linear `local` conjuncts — no longer
  erroring on the second. The dispatch arm branches: `subqs.len() == 1` stays on the
  existing `emit_exists_pieces(…, None)` path; `> 1` routes to
  `create_subquery_chain_view`. The `chain.segments.is_empty()` guard (EXISTS over a
  compiled CTE/derived-table sub-plan is unsupported) is checked once, before either path.

- **Mixed shapes & NOT.** Each segment independently classifies equi vs band vs pure-range
  and picks its own `_join_pk`/`_src_pk`; shielding projects base columns by name and is
  blind to which synthetic PK the prior segment used, so equi↔range mixes chain freely.
  Anti-join segments chain identically to semi-join segments — the `negated` flag rides
  each conjunct (`node_negated ^ outer_not`) and each conjunct is an independent filter over
  the running relation. Order follows syntactic conjunct order.

**Gate.** `make verify` + `make e2e` (W=4). The `outer_override` addition to
`emit_exists_pieces` is behaviour-neutral for the single-conjunct path — existing
`exists`/`in` unit and e2e tests are the byte-for-byte gate. New e2e tests must check
**weights**, not just row presence.

### 2. Set-op directly over a raw join, per side (low priority)

A set-op side whose FROM has joins is compiled to a hidden `H` first, exactly like the
GROUP BY / DISTINCT-over-join paths (`resolve_operator_input` → `compile_join_to_hidden`),
and the side then resolves over `H` by column name. `compile_set_op_side` already takes a
pre-resolved `source: (u64, Rc<Schema>)`; the only reason joins are rejected today is that
`emit_set_op_pieces` routes every side through `resolve_side_source`, which rejects a JOIN'd
FROM. `H` carries a leading synthetic PK (`[_join_pk/_pair_pk, cols…]`), which must be
dropped — the same shielding as part 1.

**Mechanics** (`dispatch.rs` + `set_op.rs`):

- Detect at classification a `SetOp` whose left and/or right `Select` has
  `from[0].joins` non-empty.
- For each such side: `compile_join_to_hidden` (adds `H` to `chain`, returns
  `(H_tid, H_schema)`); build a rewritten side `Select` that (a) replaces
  `from[0].relation` with `H`'s hidden name, (b) clears `from[0].joins` (so the guard is
  moot), and (c) sets its projection to select only `H`'s **payload** columns — indices
  `k..` where `k = H.schema.pk_cols.len()` — dropping `H`'s synthetic PK; `cache_alias`
  the hidden name to `(H_tid, H_schema)`.
- Thread `chain` into `emit_set_op_pieces` and run the rewritten sides through the
  existing `resolve_side_source` + `compile_set_op_side` path (a plain-relation FROM now,
  resolvable via the cache). No new `compile_set_op_side` parameter is needed.

## Out of scope

- Deeper nonlinear-over-nonlinear composition (GROUP BY / DISTINCT over a DISTINCT or
  set-op or EXISTS sub-plan). The CTE body for those is itself rejected, so each would
  need the same "compile sub-plan to a hidden view + operator-over-H" treatment; value is
  low and there is no user demand yet.
- Correlated scalar subqueries, ANY/ALL, subqueries outside top-level WHERE conjuncts
  (unchanged from the current EXISTS/IN builder's restrictions).

## Testing

- Rust: the `outer_override` addition is behaviour-neutral for the single-conjunct path —
  existing `exists`/`in` unit and e2e tests are the byte-for-byte gate.
- Python e2e (W=4), all weight-verified: `test_subquery_chain.py` — two-EXISTS (semi∘semi),
  EXISTS+NOT-EXISTS (semi∘anti), IN+EXISTS, EXISTS+linear-local, three-conjunct, an
  equi∘range mix, backfill (view created after data), DROP-cascade of the hidden segments,
  and incremental insert + retraction that flips membership at each link. A `SELECT *`
  two-EXISTS view must assert the final schema is exactly `[synthetic_pk, base cols…]` —
  no leaked `_join_pk_*`.

## Invariants preserved

- Single-source-per-epoch and the self-join guard: each segment's two sources (its running
  outer and its subquery's inner) are distinct `source_id`s; the per-segment
  `outer_tid == inner_tid` guard still fires independently for every link. A later
  conjunct's subquery may reference a base table an earlier segment already consumed
  (its immediate outer is the prior *segment*, a distinct id) — that is the
  shared-source-branches shape (distinct dependency edges → separate epochs), already
  proven correct for `t ⋈ view-over-t`.
- `MAX_CHAIN_SEGMENTS = 64` (enforced by `create_view_chain`) bounds the conjunct count;
  projection shielding keeps each segment's payload at base-outer width, so `MAX_COLUMNS`
  is never approached by chain length.
