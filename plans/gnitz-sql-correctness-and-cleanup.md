# gnitz-sql correctness and cleanup

A code-quality pass over `crates/gnitz-sql/src` (the SQL front end: parse → bind →
plan/lower for views, or → execute for DML). Shipped so far — the four correctness
fixes C1–C4, the two Theme A fixes A1–A2, the three Theme B view/binder refactors
B2–B4, the DML row-builder dedup C-DML-1, the F2 doc fix, the two Theme G perf
tweaks G1–G2, and the two Theme D type-safety changes D1 (join-key `TypeCode`) and D3
(`ColumnDef::new`) (see the "— DONE" sections below; C-DML-2 was dropped); the five
remaining changes (D2, E1, F1, F3, F4) are ordered by leverage (impact × confidence ÷
effort) and grouped into themes — duplication, type-safety, and hygiene with no
behavioural change unless noted.

Every item names exact `file:line` anchors, the change (with code), the invariants
it must not break, effort (S/M/L), value, and how to verify. Line numbers are from
the current tree and will drift as items land — re-anchor by symbol name.

---

## Correctness fixes (C1–C4) — DONE

Shipped. Each converts a silent wrong-result / data-corruption into an honest error.
Validated by pure unit tests (run under `make verify`) plus feature-gated integration
tests (`cargo test -p gnitz-sql --features integration`).

- **C1** — INSERT with an explicit column list (`dml/insert.rs`,
  `validate_insert_column_list`): accept a full in-order list (positional write is
  already correct), reject reordered/partial lists. *Refined from the audit's blanket
  reject, which would have broken ~200 existing in-order column-list INSERTs.*
- **C2** — Aggregate argument types are validated in `push_agg_specs`
  (`plan/view/group_by.rs`), the single convergence point of the SELECT-list and
  HAVING-only callers; the SELECT-path block was removed. A HAVING-only
  `SUM(blob)`/`AVG(uuid)`/`MIN(str)` now returns `Bind` instead of unevaluatable garbage.
- **C3** — Out-of-i64-range non-fractional integer literals are rejected in
  `bind_literal` (`bind/structural.rs`) instead of silently coercing to a lossy f64.
- **C4** — FK rewrites that would narrow or re-sign the child's declared type are
  rejected via `int_domain_fits` (`types.rs`) in `check_fk_type_compat` (`plan/ddl.rs`);
  the safe widening idiom (`INT` child → `BIGINT` parent) still works.

---

## Theme A (A1–A2) — DONE

Shipped. Two behavioural fixes that turn a silent wrong outcome into correct behaviour.
Validated by a pure unit test (run under `make verify`) plus feature-gated integration
tests (`cargo test -p gnitz-sql --features integration`).

- **A1** — qualified-column alias lookup in joins is now case-insensitive.
  `resolve_qualified_column` (`bind/resolve.rs`) lowercases its probe key (`to_lowercase`,
  matching the lowercased join `AliasMap` keys) so `ON A.x = b.y` against `FROM t a` binds
  under the case-preserving `GenericDialect`. One-line fix at the single resolver chokepoint.
- **A2** — a band (equi + range, n_eq ≥ 1) LEFT JOIN over a non-base-table preserved side is
  rejected with `Unsupported`, via a caller-side `client.resolve_table_id` base-table probe
  in `execute_create_join_view` (`plan/view/join.rs`) — *not* inside `build_range_join_view`,
  which has no relation-name parameter. The band null-fill `a_all − distinct(π_A(inner))`
  clamps matched multiplicity to 1, silently over-filling a bag-valued left input (e.g. a
  UNION ALL view whose projection drops the PK, collapsing duplicate projected content to one
  weight-≥2 `_set_pk` identity). Conservatively rejects *all* non-base-table left sides
  (views, CTEs) — an accepted over-approximation. Pure-range (n_eq == 0) and equi LEFT remain
  weight-exact and are unaffected. The weight-exact real fix — replacing the band `distinct`
  with a threshold-witness anti-join (CLAUDE.md "Weight-exactness") — is a known, deferred
  *gnitz-engine* change (out of scope for this gnitz-sql pass); a future pass can lift the
  rejection.

---

## Theme B (B2–B4) — DONE

Shipped. Three pure-DRY refactors in the view-compile and binder clusters; circuits
and `ExprProgram`s are byte-identical. Validated by `make verify`, the gated
`cargo test -p gnitz-sql --features integration` planner suite (all suites green),
and the W=4 `make e2e` distinct/set-op tests.

- **B2** — `compile_bound_expr` (`lower.rs`) narrowed to return `u32`; the dead
  `is_float` bit every external caller discarded is gone. New
  `compile_bound_expr_to_program` collapses the fresh-builder "compile one BoundExpr
  to a finished program" idiom at every WHERE/HAVING/residual site (`predicates`,
  `simple`, `group_by`, `set_op`). The lone register-form caller (the `simple.rs`
  projection-EMIT loop) keeps `compile_bound_expr`, detupled.
- **B3** — `single_relation_col_name` (`ast_util.rs`) shares the single-relation
  column-name AST match (`Identifier` | two-part `CompoundIdentifier`) between
  `SingleTable::idx` (`bind/structural.rs`) and `Having::bind_column`
  (`plan/view/group_by.rs`); each caller keeps its own error string and tail.
- **B4** — `execute_create_distinct_view` (`plan/view/set_op.rs`) reuses
  `compile_set_op_side`, deleting a ~30-line copy of the
  resolve→WHERE→hash-reindex→shard pipeline. Circuit byte-identical: the reused path
  uses `input_delta_tagged`, so `CircuitBuilder::new(view_id, 0)` is inert
  (`primary_source_id` never enters the built `Circuit`; `dependencies()` derives
  from `ScanDelta` nodes), exactly as the set-op view builder already does. To keep
  per-caller error context, `compile_set_op_side` / `resolve_set_projection` gained a
  `context: &str` arg (`"set operation"` vs `"SELECT DISTINCT"`), unifying the
  FROM-shape / projection rejection messages.

**B1 was dropped** (extract the WHERE-filter block into a new `plan/view/common.rs`):
after B2 each WHERE site is a 7-line `if-let-else`, so B1 would add a new file +
module to dedup just two sites — not net-simpler, an additive wrapper the codebase
avoids. The two inline WHERE blocks stay.

---

## Theme C (C-DML-1) — DONE

Shipped. `build_merged_row` (`dml/mutate.rs`) is the single merged-row builder shared
by UPDATE SET (`write_set_columns`) and ON CONFLICT DO UPDATE
(`client_side_merge_do_update`): PK from a `pk_src`, null-seed + carried columns from a
`carry_src`, a per-column `resolve` closure choosing eval-vs-carry. The extraction
deleted the drift-prone untested insert.rs copy and folded `write_set_columns`'s prior
O(cols²) per-column `assignments.iter().find` into the same O(1) `asn_by_col`
pre-index the DO-UPDATE arm already used (`resolve_set_target`'s `seen` guard rules out
duplicate column indices, so the lookup change is result-identical). A distinct-source
unit test covers the PK-from-`pk_src` / carry-from-`carry_src` asymmetry that the four
`write_set_columns` tests (now exercising `build_merged_row` transitively) miss.
Validated by `make verify`, the gated `cargo test -p gnitz-sql --features integration`
suite, and W=4 `make e2e`.

**C-DML-2 was dropped** (extract a `first_index_seek` helper for the SELECT and
UPDATE/DELETE equality-index seek loops). No clean home: `exec/residual.rs` would
violate exec's documented "holds no edge back up into dml/plan" invariant (the helper
needs `IndexSeekCandidate` from `dml/plan.rs`), and `dml/plan.rs` is deliberately
client-agnostic (a `fetch_indexes` closure, `ClientError`-typed) so hosting a
client-driving, `GnitzSqlError`-returning fn breaks its character. The change is
line-neutral, and select.rs's third structurally-identical loop (the range loop over
`seek_by_index_range`/`IndexRangeCandidate`) cannot share a `seek_by_index`-shaped
helper anyway — so the single-definition goal is unreachable. The two ~9-line loops
stay inline.

---

## Theme D — Type safety

**D1 and D3 — DONE.** Two behaviour-neutral type-safety changes; circuits and wire bytes
byte-identical. Validated by `make verify`, the gated
`cargo test -p gnitz-sql --features integration` planner suite, and the W=4 `make e2e`
join/set-op/distinct subset (210 passed).

- **D1** — the join-key common type now threads as `TypeCode` end-to-end:
  `validate_join_key_pair` / `validate_range_join_key_pair` return `Result<TypeCode>`,
  `RangeConjunct.tc` and the `JoinPredicates` eq-tcs slot are `TypeCode` /
  `Vec<TypeCode>`, and `side_target_tcs` / `build_range_join_view` take `&[TypeCode]` —
  deleting the internal `TypeCode → u8 → TypeCode` round-trip at all five
  `from_validated_u8` consumer sites. The `as u8` cast survives only inside
  `carried_reindex_tc` / `resolve_reindex_type` / `map_reindex(&[u8])`, and the `0`
  self-derive sentinel is untouched, so the wire/circuit serialization is unchanged.
- **D3** — `ColumnDef::new(name, type_code, is_nullable)` added to `gnitz_core::ColumnDef`;
  all 22 zero-FK `ColumnDef` literals in gnitz-sql (16 production + 6 test) now go through
  it, deleting the dead `fk_table_id: 0, fk_col_idx: 0` boilerplate. Synthetic names
  (`_join_pk`, `_set_pk`, `_distinct_pk`, `_group_pk`, `_agg`, `_jp`, `m`,
  `_pair_pk_{slot}`) are preserved verbatim. The engine's separate catalog `ColumnDef`
  and gnitz-core's own ~150 zero-FK literals were out of scope and left untouched.

> **D2's anchors below predate D1/D3** and have drifted; re-anchor by symbol name.

### D2 — Newtype the three join-column coordinate spaces

- **Files:** `plan/view/predicates.rs:180-194, 247-300, 429-471`; consumers in
  `plan/view/join.rs`.
- **Problem:** JOIN-ON classification threads three semantically different `usize`
  index spaces through one untyped type, with manual `+left_n` / `-left_n` / `+base`
  arithmetic and nothing preventing a mix-up: (1) `resolve_join_col_ref` returns a
  GLOBAL A‖B index; (2) `cross_table_pair` converts to TABLE-RELATIVE (`r - left_n`,
  predicates.rs:186-194), stored in `RangeConjunct.left_col/right_col` and the
  `left_cols`/`right_cols` of `JoinPredicates`; (3) `JoinResidual.idx` shifts a
  global index into MERGED-OUTPUT space (`+ base`). A function handed a
  table-relative index where it expects a global one compiles and silently reads the
  wrong column — the column-index-confusion bug class. This is the strongest
  "make-illegal-states-unrepresentable" opportunity in the crate.
- **Change:** Introduce three one-field newtypes (e.g. in predicates.rs or a small
  `plan/view/coords.rs`):

  ```rust
  #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub(crate) struct GlobalColIdx(pub(crate) usize);   // index into A‖B
  #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub(crate) struct TableRelIdx(pub(crate) usize);    // index within one side's schema
  #[derive(Clone, Copy, PartialEq, Eq, Debug)]
  pub(crate) struct MergedColIdx(pub(crate) usize);   // index into the join output
  ```

  `resolve_join_col_ref` returns `GlobalColIdx`; `cross_table_pair` consumes
  `GlobalColIdx` and returns `(TableRelIdx, TableRelIdx, bool)` (the `- left_n`
  conversions become its only space-crossing); `RangeConjunct`/`JoinPredicates`
  store `TableRelIdx`; `JoinResidual.idx` returns `MergedColIdx` (the `+ base`
  conversion). Unwrap to `usize`/`u16` only at the `map_reindex`/`ExprBuilder`
  boundary. A mismatched argument now fails to compile.
- **Invariants:** Wire/engine semantics unchanged — purely the in-planner index
  representation. Keep the per-space comments (predicates.rs:180-194, 234-240) as
  the human-readable companions to the now-enforced types.
- **Effort:** M (mechanical but touches every join-key build/consume site in
  predicates.rs and join.rs). **Value:** Medium — makes a silent-wrong-column class
  unrepresentable; the audit's clearest illegal-states-unrepresentable win.
- **Verify:** `make verify` (it must compile through every conversion); the join
  cluster's existing tests pass unchanged.

---

## Theme E — DDL validation dedup

### E1 — Share the key-eligibility rejection between PRIMARY KEY and UNIQUE

- **Files:** `plan/ddl.rs:406-419, 457-469`; new helper in `plan/validate.rs`.
- **Problem:** Two loops perform the identical gate `if !tc.is_pk_eligible() {
  return Err(Unsupported(format!(...))) }`, differing only in the role noun
  ("PRIMARY KEY" vs "UNIQUE") and the trailing prose. The allow-list, rejected-type
  message, and error variant are copy-pasted, so a change to the rule or wording
  must be made in two places.
- **Change:** Extract a role-parameterised helper in `validate.rs`, beside the
  `reject_float_key(col, role)` family it mirrors:

  ```rust
  /// Reject a column type that cannot back a hashed key (PRIMARY KEY or UNIQUE
  /// index). `role` names the clause for the message. `is_pk_eligible` is the
  /// shared allow-list (fixed-width integer, U128, UUID).
  pub(crate) fn reject_non_key_eligible(name: &str, tc: TypeCode, role: &str) -> Result<(), GnitzSqlError> {
      if !tc.is_pk_eligible() {
          return Err(GnitzSqlError::Unsupported(format!(
              "{role} column '{name}' of type {tc:?} is not supported \
               ({role} must be a fixed-width integer, U128, or UUID column; \
               String, Blob, and float columns cannot be a {role} key)"
          )));
      }
      Ok(())
  }
  ```

  Call it from both loops (`reject_non_key_eligible(&cols[i].name, tc, "PRIMARY KEY")?`
  and `… , "UNIQUE")?`).
- **Invariants:** None — `is_pk_eligible` is unchanged; the two messages converge to
  one role-parameterised template. A test pinning the exact old phrasing updates.
- **Effort:** S. **Value:** Low — clean dedup co-located with the existing
  `reject_float_key` helpers.
- **Verify:** `make test` (ddl tests); PK and UNIQUE rejections still fire for
  String/Blob/float columns.

---

## Theme F — Comment and signature hygiene

### F1 — Fix the stale InterpBackend doc (unsigned columns are zero-extended)

- **Files:** `exec/eval.rs:20-24`
- **Problem:** The doc claims the backend reproduces a "signed-`i64` decode of
  unsigned columns (a separate, tracked defect)". False: narrow unsigned columns
  (U8/U16/U32) are zero-extended via `FixedInt::decode_le_i64`, and the module's own
  tests (`test_u8_200`/`u16_60000`/`u32_3b_not_sign_extended`, eval.rs:240-265)
  assert exactly that. The only lossy case is full-width U64 (bit-cast: `U64::MAX →
  -1i64`); U128/UUID/STRING/BLOB are rejected.
- **Change:** Rewrite eval.rs:20-24 to describe the real behaviour — narrow unsigned
  zero-extended, full-width U64 bit-cast, U128/UUID/STRING/BLOB rejected — wording
  aligned with the existing tests so it cannot drift again. Comment-only.
- **Invariants:** None — corrects a false description.
- **Effort:** S. **Value:** Low — removes a misdirecting comment.
- **Verify:** Read-through against eval.rs:240-265; no behavioural change.

### F2 — Fix the ConflictPlan doc — DONE

Shipped. The `enum ConflictPlan` header doc (`dml/insert.rs`) no longer claims a
"resolved target / reserved slot for future ON CONSTRAINT … not implemented in v1"
(the enum holds three target-free dispositions; the target is validated and discarded
in `validate_conflict_target`). The forbidden future-work framing is gone. The three
variant docs were already accurate and were left as-is. Comment-only.

### F3 — Route INSERT integer encoding through `parse_pk_literal_packed`

- **Files:** `codec/colwrite.rs:87-101`
- **Problem:** The integer arm of `append_value_to_col` (colwrite.rs:92-101)
  hand-duplicates the narrow-integer branch of `parse_pk_literal_packed` (parse →
  `FixedInt::range` check → pack). `pk_codec` is the documented single source of
  truth for INSERT/SEEK literal acceptance, and the colwrite comment (88-89) admits
  the coupling is maintained by hand. A future range/sign-policy change in
  `parse_pk_literal_packed` would silently desync the value path from PK routing.
- **Change:** Add `parse_pk_literal_packed` to the existing `use
  crate::codec::pk_codec::…` import; replace the `_` integer arm:

  ```rust
  _ => {
      // Single source of truth for narrow-int literal acceptance (pk_codec); `n`
      // already carries any leading '-', so negated = false. Emit the low
      // wire_stride bytes of the packed u128.
      let packed = parse_pk_literal_packed(tc, n, false)
          .ok_or_else(|| GnitzSqlError::Bind(format!("{tc:?} value out of range: {n}")))?;
      buf.extend_from_slice(&packed.to_le_bytes()[..tc.wire_stride()]);
  }
  ```

  For narrow ints `width() == wire_stride()`, so bytes are byte-identical. F32/F64
  are intercepted by the arms above; U128/UUID/I128 never back a `ColData::Fixed`
  column, so they never reach this arm.
- **Invariants:** The value path and PK-routing path must accept/reject identically —
  this is what enforces it. Negation is folded into `n` before this arm.
- **Effort:** S. **Value:** Low — eliminates the desync hazard between INSERT-value
  and PK-seek literal policy.
- **Verify:** `cargo test -p gnitz-sql codec::colwrite` — the encoding/range tests
  assert bytes and `.is_err()`, not exact text, so they pass.

### F4 — Drop the unused `&self` on `Binder::bind_expr`

- **Files:** `bind/structural.rs:16-19` and its call sites.
- **Problem:** `bind_expr(&self, …)` never uses `&self` — it forwards to
  `bind_structural(expr, &SingleTable { schema })`; context is fully determined by
  `(expr, schema)`. The "retain the receiver for future state" rationale is
  forbidden speculative generality.
- **Change:** Make it a free `pub(crate) fn bind_single_table(expr, schema)` in
  structural.rs (drop the receiver and the comment), update the call sites, and drop
  the now-unused binder param from `residual.rs::bind_residuals` if it becomes
  unused.
- **Invariants:** BoundExpr coverage-parity walk unchanged (only the entry signature
  moves). Sequence after B1 and C-DML-1 (which call `binder.bind_expr`), or update
  those calls in the same edit.
- **Effort:** S. **Value:** Low — removes speculative generality.
- **Verify:** `make verify`; all binder tests pass.

---

## Theme G — DONE

Shipped, both behaviour-neutral. **G1** — `apply_residual_filter` (`exec/residual.rs`)
collects passing indices first and returns the fetched batch unmoved when every row
passes, skipping the full per-row `copy_batch_row` clone on a broad residual (verified
byte-identical: `copy_batch_row` copies weight/null/payload verbatim, no
ghost-drop/consolidate/re-encode). **G2** — `resolve_residual_rows` (`dml/mutate.rs`)
reserves `matched` to `batch.pks.len()`.

---

## Sequencing

- **C1–C4** and **B2–B4** — DONE; **B1 dropped**. **C-DML-1**, **F2**, **G1–G2**,
  **D1**, **D3** — DONE; **C-DML-2 dropped**.
- **D2** (coordinate newtypes) is now next — the largest remaining item. Its anchors in
  the Theme D section predate the D1/D3 landings and have drifted; re-anchor by symbol
  name. The join-key type plumbing it builds on is already simplified.
- **E1**, **F1**, **F3** are independent and can land in any order.
- **F4** (drop `&self` on `bind_expr`) must update every `binder.bind_expr` call site
  in the same edit — the inline WHERE filters in the view builders (`set_op`,
  `group_by`, `simple`).

---

## Considered and excluded

Findings raised during the audit and rejected on inspection — **do not implement.**

- **`infer_type`'s `AggCall` arm is "dead + a latent SUM/AVG-over-float mistyping" →
  mark `unreachable!()`.** The arm IS reached (`SELECT SUM(x) FROM t` classifies as a
  simple view and `project_schema.rs` calls `bound.infer_type` on the AggCall node).
  The SUM/AVG-over-float type drift (ir.rs:61 catch-all `_ => I64`) is never
  observable: the view fails first at `compile_bound_expr → OpcodeBackend::agg_call`
  ("aggregate function not allowed in expression context") and is never persisted.
  `unreachable!()` would turn a clean error into a panic.

- **`eval_do_update_rhs` is a thin wrapper → inline it.** Each `BoundUpdateExpr`
  variant carries its own `expr`, so the "honest inline" just relocates the two-arm
  match into the merge loop verbatim — no behavioural or efficiency gain, and it
  trades a self-documenting named helper for inline noise.

- **Residual predicates bound against the catalog schema but evaluated against the
  reply `actual_schema` → add a `debug_assert` layout-match guard.** The divergence
  cannot occur: seek/scan reply schemas are always the server's single authoritative
  descriptor for that `target_id` (full column set, catalog order), and the binding
  schema derives from the same definition. The "reply overrides catalog" comment
  means *prefer authoritative server metadata*, differing only in canonical
  nullability — never in column-index layout. The assert would guard a property true
  by construction.

- **The three `col OP literal` recognizers re-implement a column-side/literal
  extraction skeleton → factor a shared matcher.** Their divergence (the flipped-side
  flag, UUID acceptance) is intentional; over-factoring obscures it. Revisit only if
  a fourth recognizer appears.

- **`AccessPath::Filtered` re-flattens its conjuncts downstream / defers index-vs-scan
  to runtime → restructure.** `flatten_conjuncts` is cheap on tiny WHERE vectors (not
  a hot path) and the runtime index-vs-scan deferral is correct (index liveness needs
  a round-trip). Both are low-value churn touching `pub(crate)` signatures.

- **`GnitzSqlError`'s `Bind`/`Plan`/`Unsupported` split is stringly-typed → collapse
  the variants.** A breaking change to the capi/py surface and to E2E error-text
  assertions, for a speculative taste improvement with no defect behind it.

- **O(n²) `consumed.contains(i)` / UNIQUE duplicate detection via repeated linear
  scans → switch to a `HashSet`.** Inputs are tiny (a handful of conjuncts / index
  columns); a `HashSet`'s allocation dominates at realistic sizes and the linear form
  is more readable.

- **Cold-path CREATE VIEW allocations (residual temp-schema clone; equi vs range
  `out_cols` built twice) → dedup/avoid.** Compile-time, one-shot; the worthwhile
  dedup is narrow and risks the byte-stable synthetic catalog names.
