# gnitz-sql correctness and cleanup

A code-quality pass over `crates/gnitz-sql/src` (the SQL front end: parse → bind →
plan/lower for views, or → execute for DML). Twenty-five concrete changes, ordered
by leverage (impact × confidence ÷ effort) and grouped into themes. The four
correctness fixes change query results or accepted DDL and ship first; everything
after is duplication, type-safety, and hygiene with no behavioural change unless
noted.

Every item names exact `file:line` anchors, the change (with code), the invariants
it must not break, effort (S/M/L), value, and how to verify. Line numbers are from
the current tree and will drift as items land — re-anchor by symbol name.

---

## Correctness fixes (do first)

### C1 — Reject INSERT with an explicit column list instead of silently mis-mapping

- **Files:** `dml/insert.rs:141-168, 381-403`
- **Problem:** `extract_insert_parts` (insert.rs:381-403) destructures
  `Statement::Insert(insert)` and returns `(table_name, rows, insert.on.as_ref())`
  — it never reads `insert.columns`. INSERT runs purely positionally in schema
  order (`schema.payload_columns()` / `row[ci]`, lines 159-166). The only arity
  guard (lines 146-153) compares the VALUES-tuple arity to the *table* column
  count, not to the column list. So `INSERT INTO t (b, a) VALUES (1, 2)` on a
  two-column table passes the guard and writes `a=1, b=2` — values silently
  swapped, no error. A shorter-than-arity list is incidentally caught by the arity
  guard with a misleading message. The comment at insert.rs:142-145 states
  positional-only as an *unenforced* assumption.
- **Change:** In the `Statement::Insert(insert)` arm of `extract_insert_parts`,
  before extracting the source, reject a non-empty column list:

  ```rust
  Statement::Insert(insert) => {
      if !insert.columns.is_empty() {
          return Err(GnitzSqlError::Unsupported(
              "INSERT with an explicit column list is not supported; \
               supply all columns positionally in schema order".to_string(),
          ));
      }
      let table_name = match &insert.table { /* unchanged */ };
      // ... unchanged ...
  }
  ```

  `GnitzSqlError::Unsupported(String)` is already used twice in this function
  (lines 387, 396). Drop the now-redundant "(no column list)" parenthetical from
  the insert.rs:142-145 comment, since the assumption is now enforced.
- **Invariants:** None broken — this *enforces* the documented full-row-positional
  contract (insert.rs:142-145) that was previously only assumed.
- **Effort:** S. **Value:** High — silent data corruption on a routine shape,
  converted to an honest error.
- **Verify:** Unit test: `INSERT INTO t (b, a) VALUES (1, 2)` (list length == table
  arity, order differs) returns `Unsupported`; plain `INSERT INTO t VALUES (1, 2)`
  still succeeds.

### C2 — Validate aggregate argument types on the HAVING-only path

- **Files:** `plan/view/group_by.rs:232-258, 677-743, 745-769, 296-298` and the
  import at `group_by.rs:12`; leave `bind/structural.rs:230-244` intact.
- **Problem:** Both aggregate argument-type guards are SELECT-path only — the
  SUM/AVG numeric/non-wide-int block (group_by.rs:236-250, walking
  `BoundExpr::AggCall`) and the MIN/MAX `is_min_max_orderable` check
  (structural.rs:230-244, in `SingleTable::bind_function`). An aggregate referenced
  *only* by HAVING (absent from SELECT) is materialised via `collect_having_aggs →
  having_agg_func → append_having_agg → push_agg_specs` with no type check, because
  the HAVING leaf binder never calls `SingleTable::bind_function`. A view like
  `GROUP BY k HAVING SUM(blob_col) > 0` (or `AVG(uuid_col)`, `MIN(str_col)`) ships
  an `agg_spec` the engine cannot evaluate; the engine delegates this rejection to
  the planner. Result for an unsupported type: STRING → silent garbage in all
  builds (the i64 comparator reads the 8-byte German-string descriptor prefix as a
  signed int); wide-int (U128/UUID/I128) or BLOB → debug panic / release garbage.
  Aggregates present in *both* SELECT and HAVING are already validated
  (`collect_having_aggs` dedups against `agg_mappings`); only the HAVING-only case
  escapes.
- **Change:** Lift validation into `push_agg_specs` (group_by.rs:677) — the single
  convergence point of both callers — then delete the SELECT-path block. After the
  existing wildcard guard:

  ```rust
  if let Some(c) = arg_col {
      let tc = schema.columns[c].type_code;
      match agg_func {
          AggFunc::Sum | AggFunc::Avg => {
              if !(is_integer_type(tc) || tc.is_float()) || is_wide_int(tc) {
                  return Err(GnitzSqlError::Bind(format!(
                      "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                      schema.columns[c].name,
                  )));
              }
          }
          AggFunc::Min | AggFunc::Max => {
              if !is_min_max_orderable(tc) {
                  return Err(GnitzSqlError::Bind(format!(
                      "{agg_func:?} is not supported on column type {tc:?} ('{}')",
                      schema.columns[c].name,
                  )));
              }
          }
          AggFunc::Count | AggFunc::CountNonNull => {}
      }
  }
  ```

  Add `is_min_max_orderable` to the `use crate::types::…` import at group_by.rs:12;
  delete the SELECT-path validation at group_by.rs:232-258. Leave
  `SingleTable::bind_function`'s MIN/MAX check (structural.rs:230-244) — it guards
  non-GROUP-BY SELECT-list MIN/MAX and carries its own test. GROUP-BY-SELECT MIN/MAX
  is now double-checked (binder + push_agg_specs); harmless.
- **Invariants:** `push_agg_specs` is the documented single source of truth for spec
  layout (group_by.rs:669-686) and already carries one cross-cutting guard (the
  wildcard check); validation belongs there. Do not break the SELECT-path
  `BoundExpr::AggCall` rejection `bind_function` still performs.
- **Effort:** S. **Value:** Medium-high — silent corruption / unevaluatable circuit
  on a real shape, converging two callers onto one validated gate.
- **Verify:** `CREATE VIEW … GROUP BY k HAVING SUM(blob_col) > 0` (plus
  `AVG(uuid_col)`, `MIN(str_col)`) must return `Bind`. The lifted MIN/MAX error
  variant is now `Bind` (was `Unsupported`); a GROUP-BY-level test asserting the
  variant must expect `Bind`. The existing `SingleTable` MIN/MAX test is unaffected.

### C3 — Reject out-of-i64-range integer literals instead of coercing to lossy f64

- **Files:** `bind/structural.rs:129-142`
- **Problem:** `bind_literal` does `n.parse::<i64>().map(LitInt).or_else(|_|
  n.parse::<f64>().map(LitFloat))`. A non-fractional literal that overflows i64 but
  the user meant as an integer (e.g. `18446744073709551615` = `u64::MAX`) fails the
  i64 parse, succeeds the f64 parse, and becomes `LitFloat`. `infer_type` types
  `LitFloat` as F64 (ir.rs:31); the opcode lowerer casts the integer column
  int→float and emits `fcmp_eq` (lower.rs:212-220). f64's 52-bit mantissa cannot
  represent `u64::MAX`, so `u64col = 18446744073709551615` runs a lossy float
  compare and matches wrong rows. Blast radius is narrow: only `bind_structural`
  (WHERE / projection / residual); PK-seek (`parse_literal_i128`) and INSERT
  colwrite already range-check via i128.
- **Change:** Rewrite the `Value::Number` arm so a non-fractional overflowing
  literal errors and only genuinely fractional text takes the f64 path:

  ```rust
  Value::Number(n, _) => {
      if let Ok(i) = n.parse::<i64>() {
          Ok(BoundExpr::LitInt(i))
      } else if n.contains(['.', 'e', 'E']) {
          n.parse::<f64>()
              .map(BoundExpr::LitFloat)
              .map_err(|_| GnitzSqlError::Bind(format!("invalid number literal: {n}")))
      } else {
          // Non-fractional literal that overflows i64. BoundExpr::LitInt is
          // i64-only, so it cannot be represented — reject rather than run an
          // integer-column comparison through lossy f64.
          Err(GnitzSqlError::Bind(format!("integer literal out of range: {n}")))
      }
  }
  ```

  `BoundExpr::LitInt` is i64-only (ir.rs:16). Negative literals arrive as
  `UnaryOp(Neg, LitInt(digits))`, so only large positive magnitudes reach this arm.
- **Invariants:** PK byte-equality / OPK packing unaffected (PK paths range-check
  separately). Replace the `// Try integer first, then float.` comment with the
  rationale above.
- **Effort:** S. **Value:** Medium — wrong rows on wide-unsigned comparisons;
  contained fix.
- **Verify:** `bind_literal(Number("18446744073709551615"))` returns `Bind`; `"1.5"`
  / `"1e3"` still bind `LitFloat`; `"42"` binds `LitInt`.

### C4 — Reject FK rewrites that narrow or re-sign the child column's declared type

- **Files:** `plan/ddl.rs:15-31` (`check_fk_type_compat`), call sites at ddl.rs:285-297
  and 332-344; one new predicate in `types.rs`.
- **Problem:** `check_fk_type_compat` (ddl.rs:18-31) treats *any* integer child as
  compatible with *any* integer parent (`is_integer_type` ignores width and
  signedness). After the check, the child column's declared type is overwritten with
  the parent's (`cols[i].type_code = parent_pk_type`, ddl.rs:297, 344) — the engine
  requires the FK column to encode byte-identically to the parent PK, so adopting
  the parent type is intentional. But when the parent is *narrower* or
  *differently-signed* than the declared child (e.g. child `BIGINT UNSIGNED`/U64
  referencing parent `INT`/I32), the column is silently narrowed/re-signed: the
  user's declared domain is discarded, and values legal under the declared type are
  later rejected at INSERT with a message naming the wrong type. (The value path
  range-checks against the rewritten type, so this is silent declared-type narrowing
  and confusing rejection, not on-disk corruption.) The standalone widening case
  (`INT` child → `BIGINT` parent) is genuinely safe and must keep working.

  Note: the only safe rewrite is `child → parent` when **parent can represent every
  value of the child's declared type**. The intuitive "require child width ≥ parent"
  rule is backwards — that is the narrowing case being closed.
- **Change:** Add a domain-subset predicate in `types.rs` (its stated home for
  type-membership tests), then gate the integer branch on it:

  ```rust
  // types.rs — alongside is_integer_type / is_wide_int.
  /// True for the signed integer TypeCodes (I8..I128); false for unsigned.
  /// Only valid for `is_integer_type(tc)` inputs.
  pub(crate) fn int_is_signed(tc: TypeCode) -> bool {
      matches!(tc, TypeCode::I8 | TypeCode::I16 | TypeCode::I32 | TypeCode::I64 | TypeCode::I128)
  }

  /// True iff every value of integer type `child` is representable in integer
  /// type `parent` — so rewriting an FK child column to the parent's type loses
  /// no value the child could legally hold.
  pub(crate) fn int_domain_fits(child: TypeCode, parent: TypeCode) -> bool {
      let (cw, pw) = (child.wire_stride(), parent.wire_stride());
      match (int_is_signed(child), int_is_signed(parent)) {
          (false, false) | (true, true) => cw <= pw, // same signedness: parent ≥ child width
          (false, true) => cw < pw,                  // unsigned child needs a strictly wider signed parent
          (true, false) => false,                    // signed child has negatives no unsigned parent holds
      }
  }
  ```

  ```rust
  // ddl.rs — check_fk_type_compat
  let is_compat = if is_integer_type(fk_col_type) && is_integer_type(parent_col_type) {
      int_domain_fits(fk_col_type, parent_col_type)
  } else {
      fk_col_type == parent_col_type
  };
  if !is_compat {
      return Err(GnitzSqlError::Bind(format!(
          "FK type mismatch: column type {fk_col_type:?} cannot reference column type \
           {parent_col_type:?} — the child column adopts the referenced column's type, \
           which would narrow or re-sign {fk_col_type:?}; declare the child as \
           {parent_col_type:?} or a narrower compatible integer",
      )));
  }
  ```

  `wire_stride()` is 1/2/4/8 for narrow ints and 16 for U128/I128; UUID is not an
  `is_integer_type`, so a UUID FK still falls to the exact-match arm. Update the
  ddl.rs:15-17 doc to "an integer child widens to an integer parent whose domain
  covers it; otherwise the types must match exactly."
- **Invariants:** The child column must still end up typed as the parent PK (the
  engine's FK encoding contract); this only restricts *which declared child types*
  are accepted. Same error variant (`Bind`) and shared by both resolver paths.
- **Effort:** M. **Value:** Medium — closes silent declared-type narrowing on FK
  DDL while preserving the safe widening idiom.
- **Verify:** `CREATE TABLE c (id U64 PK, p BIGINT UNSIGNED REFERENCES parent_i32(id))`
  now returns `Bind`; `… p INT REFERENCES parent_bigint(id)` (true widen) still
  succeeds and the child column ends up `I64`. Any existing test asserting a
  wide-child→narrow-parent FK succeeds is encoding the bug and must flip to expect
  `Bind`.

---

## Theme A — Resolution and join null-fill robustness

### A1 — Case-insensitive qualified-column alias lookup

- **Files:** `bind/resolve.rs:49-60`
- **Problem:** `resolve_qualified_column` probes `tables.get(table_alias)` with the
  raw AST identifier, but every join `AliasMap` is keyed by lowercased aliases
  (join.rs inserts `left_alias.to_lowercase()` / `right_alias.to_lowercase()`).
  Under `GenericDialect` (case-preserving), `ON A.x = b.y` against `FROM t a` misses
  the map and errors "table alias 'A' not found" — while the *column* half of the
  same reference resolves case-insensitively via `find_unique_column`'s
  `eq_ignore_ascii_case`. This is the lone case-sensitive lookup in the resolver.
- **Change:** Lowercase only the probe key, leaving `table_alias` in the message:

  ```rust
  let rel = tables
      .get(&table_alias.to_ascii_lowercase())
      .ok_or_else(|| GnitzSqlError::Bind(format!("table alias '{table_alias}' not found")))?;
  ```

- **Invariants:** `resolve_qualified_column` is the join-only multi-table resolver
  (its doc says so); every `AliasMap` reaching it is built with lowercased keys, so
  lowercasing the probe is safe for all of them. The `Binder.cache` (raw-keyed) is
  never passed here. Keep `find_unique_column` case-insensitive.
- **Effort:** S. **Value:** Medium — internally inconsistent and SQL-incorrect;
  one-line fix.
- **Verify:** Unit test: `AliasMap` keyed `"a"`, `resolve_qualified_column("A", …)`
  resolves. E2E: `CREATE VIEW v AS SELECT * FROM t a JOIN u b ON A.x = b.y` binds.

### A2 — Reject band LEFT JOIN over a non-base-table (bag-valued) preserved side

- **Files:** `plan/view/join.rs` — band-LEFT path in `build_range_join_view`
  (~join.rs:537, the existing `n_eq == 0` range-type guard).
- **Problem:** A band (n_eq ≥ 1) LEFT-outer null-fill is built as
  `a_all − distinct(π_A(inner))`. `distinct` clamps each matched preserved-row
  identity to weight 1 while `a_all` retains its true weight `w`. This is
  weight-exact only when preserved-side identities are unique. A `UNION ALL` view
  carries a single synthetic hashed `_set_pk` U128 (set_op.rs:262-268), so two
  source rows whose projected payload collides within a branch produce one identity
  of weight 2 — a genuine bag. A matched weight-`w` left row then leaks a spurious
  weight-`(w−1)` null-fill row. The binder accepts views as the preserved side, and
  `build_range_join_view` has no unique-PK guard. The sibling pure-range (threshold)
  and equi (anti-join) null-fill paths are already weight-exact; band is the lone
  outlier.
- **Change:** Reject the unsupported shape with a clear error. Classify the preserved
  (left) source as a base table with a table-only probe — `client.resolve_table_id`
  consults `TABLE_TAB` only, so a view name misses it (the same discriminator
  `resolve_base_table` already uses). At the band-LEFT branch:

  ```rust
  if is_left_join && n_eq >= 1 {
      // A view's preserved side may be bag-valued (e.g. UNION ALL's synthetic
      // content-hash PK can collide), and the band null-fill `a_all − distinct(...)`
      // clamps matched multiplicity to 1, over-filling such a side. Only base
      // tables are unique_pk-guaranteed.
      if client.resolve_table_id(schema_name, left_table_name).is_err() {
          return Err(GnitzSqlError::Unsupported(
              "band LEFT JOIN requires a base table on the preserved (left) side; \
               its null-fill clamps matched multiplicity to 1, over-filling a \
               bag-valued (view) input — use INNER JOIN or a base-table left side".into(),
          ));
      }
  }
  ```

  This stays entirely within gnitz-sql (one compile-time probe on a rare shape) and
  newly rejects a currently-miscomputing query — a strict improvement.
- **Invariants:** Z-set weight-exactness. The load-bearing comment at join.rs:840
  (`a.pk unique → never spans workers`) is a *partitioning* claim, not a
  multiplicity claim — do not conflate. The conservative rejection of *any* view
  (even a simple unique-PK-preserving one) is acceptable: band LEFT JOIN over a view
  is exotic, and correctness beats permissiveness here.
- **Effort:** M. **Value:** Medium — closes a silent wrong-weight result.
- **Verify:** E2E: band LEFT JOIN with a `UNION ALL` view on the preserved side now
  returns `Unsupported`; band LEFT JOIN over a base table still compiles and
  produces correct weights.

---

## Theme B — Shared view-compile and predicate helpers

Pure-DRY refactors in the view and binder clusters; generated programs are
byte-identical unless noted.

### B1 — Extract the optional-WHERE-filter block (3 identical sites)

- **Files:** `plan/view/set_op.rs:33-41, 318-326`; `plan/view/group_by.rs:369-377`.
  Leave `simple.rs:27-34`.
- **Problem:** Three single-source view builders open-code a byte-identical block:
  `bind_expr → new ExprBuilder → compile_bound_expr → eb.build(reg) → cb.filter(inp,
  Some(prog)) else inp`. Any change to WHERE-filter lowering must edit three sites.
- **Change:** Add a shared helper in a new `plan/view/common.rs` (`mod common;` in
  `plan/view/mod.rs`):

  ```rust
  pub(crate) fn build_optional_filter(
      binder: &mut Binder<'_>,
      cb: &mut CircuitBuilder,
      inp: gnitz_core::NodeId,
      selection: Option<&sqlparser::ast::Expr>,
      schema: &gnitz_core::Schema,
  ) -> Result<gnitz_core::NodeId, GnitzSqlError> {
      let Some(where_expr) = selection else { return Ok(inp) };
      let bound = binder.bind_expr(where_expr, schema)?;
      Ok(cb.filter(inp, Some(compile_bound_expr_to_program(&bound, schema)?)))
  }
  ```

  (uses B2's `compile_bound_expr_to_program`). Replace the three blocks with one
  call each.
- **Invariants:** Do not fold in `simple.rs:27-34` — it returns `Option<ExprProgram>`
  and defers `cb.filter` because projection analysis and node ordering interpose. Do
  not touch `group_by.rs:597-598` (post-reduce HAVING filter on `reduced`) or the
  join filters.
- **Effort:** S. **Value:** Low — the most-repeated exact block in the view cluster.
- **Verify:** `make verify`; programs identical, no test change.

### B2 — Add `compile_bound_expr_to_program` and narrow `compile_bound_expr` to `u32`

Merges two findings at one boundary so the call sites are touched once.

- **Files:** `lower.rs:281-291`; call sites `predicates.rs:78-80, 498-500`,
  `simple.rs:29-31`, `group_by.rs:371-373, 595-597`, `set_op.rs:35-37, 320-322`.
- **Problem:** (a) A 3-line "compile one BoundExpr to a finished `ExprProgram`,
  discard `is_float`" idiom repeats verbatim at the single-expression sites. (b) The
  public `compile_bound_expr` returns `Result<(u32, bool), _>`, exposing an
  `is_float` bit meaningful only inside `OpcodeBackend`'s recursion; every external
  caller discards it via `(reg, _)`. The internal `lower_bound_expr` walk — which
  actually consumes float-ness — does not go through the public wrapper, so
  narrowing it cannot affect float-cast logic.
- **Change:** Narrow the wrapper and add the program helper:

  ```rust
  pub(crate) fn compile_bound_expr(
      expr: &BoundExpr, schema: &Schema, eb: &mut ExprBuilder,
  ) -> Result<u32, GnitzSqlError> {
      let mut backend = OpcodeBackend { schema, eb };
      lower_bound_expr(expr, &mut backend).map(|(reg, _)| reg)
  }

  pub(crate) fn compile_bound_expr_to_program(
      expr: &BoundExpr, schema: &Schema,
  ) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
      let mut eb = ExprBuilder::new();
      let reg = compile_bound_expr(expr, schema, &mut eb)?;
      Ok(eb.build(reg))
  }
  ```

  Replace the single-expression dances with `compile_bound_expr_to_program(…)`.
  Multi-instruction register-form callers (projection EMIT at simple.rs:90,
  post-reduce map at group_by.rs:418, copy_col reindex at join.rs:883 /
  predicates.rs:89) keep `compile_bound_expr` and bind `let reg = …?;` (no longer a
  tuple). Drop the `is_float` clause from the lower.rs:281-283 doc.
- **Invariants:** Do not touch the internal `lower_bound_expr` recursion or
  `binop`/`unop` float handling (`Out = (u32, bool)`). The BoundExprBackend
  coverage-parity invariant is untouched. The simple.rs:90 EMIT path writes raw
  register bits and needs no float bit (its line-91 comment).
- **Effort:** S. **Value:** Low — DRY + API hygiene, zero behavioural change.
- **Verify:** `make verify` (clippy is warnings-as-errors); programs identical.

### B3 — Extract single-relation column-name extraction (WHERE/residual ∥ HAVING)

- **Files:** `bind/structural.rs:168-176`; `plan/view/group_by.rs:865-880`.
- **Problem:** The AST-shape extraction `Identifier(id) => &id.value` | 2-part
  `CompoundIdentifier(p) => &p[1].value`, else error, is duplicated verbatim between
  `SingleTable::idx` (structural.rs:169-173) and `Having::bind_column`
  (group_by.rs:870-878). Both encode the same single-relation rule and feed
  `find_unique_column`; only the error string and post-lookup handling
  (Having's GROUP-BY membership + reduce-position remap) legitimately differ.
- **Change:** Add a shared extractor (in `ast_util.rs`, or next to `SingleTable`)
  returning only the bare name:

  ```rust
  /// Bare column name of a single-relation reference: a plain `Identifier`, or a
  /// two-part `CompoundIdentifier` whose qualifier adds no disambiguation over a
  /// single grouped/base relation. `None` for any other shape, so each caller can
  /// raise its own context-specific error.
  pub(crate) fn single_relation_col_name(e: &Expr) -> Option<&str> {
      match e {
          Expr::Identifier(id) => Some(&id.value),
          Expr::CompoundIdentifier(p) if p.len() == 2 => Some(&p[1].value),
          _ => None,
      }
  }
  ```

  Both sites call it via `.ok_or_else(|| <their own error>)?`, keeping their
  distinct error strings and (for Having) the membership/remap tail.
- **Invariants:** Do not fold in the `Expr::Identifier`-only `find_unique_column`
  callers (dml/plan.rs:164/451/495, exec/batch.rs:35, ddl.rs:480/608,
  group_by.rs:176/983) — they don't handle the compound case; a distinct simpler
  fragment.
- **Effort:** S. **Value:** Low — two-site DRY of an AST-shape rule that slipped past
  the LeafBinder abstraction.
- **Verify:** Existing WHERE/residual and HAVING binder tests pass; both shapes still
  accepted.

### B4 — Have `execute_create_distinct_view` reuse `compile_set_op_side`

- **Files:** `plan/view/set_op.rs:297-360` (vs `15-61`).
- **Problem:** `execute_create_distinct_view` (set_op.rs:297-360) duplicates the full
  body of `compile_set_op_side` (15-61): the single-table/no-JOIN guard, `resolve`,
  input-delta, optional WHERE filter, `resolve_set_projection`, `map_hash_row`, and
  `shard([0])`; it then just appends `cb.distinct(sharded)`. The two differ only in
  `CircuitBuilder::new(view_id, source_tid) + input_delta()` vs
  `new(view_id, 0) + input_delta_tagged(source_tid)` — and these produce the
  identical `ScanDelta(source_tid)` node (in gnitz-core, `primary_source_id` is
  merely the implicit source for `input_delta()`; both emit `OPCODE_SCAN_DELTA` with
  the source in the node row). Two copies of the hash-reindex-and-shard pipeline mean
  a fix to one can miss the other.
- **Change:** Build `cb` as `CircuitBuilder::new(view_id, 0)`, call
  `compile_set_op_side(client, select, binder, &mut cb, 0)` for
  `(sharded, proj_cols, _tid)`, then `cb.distinct(sharded)`, sink, and emit the
  `_distinct_pk` U128 + `proj_cols` output schema as today.
- **Invariants:** Circuit equivalence rests on the set-op path already using
  `new(view_id, 0)` + `input_delta_tagged` for structurally-identical single-table
  sides — `primary_source_id` is consumed only by `input_delta()`, which the reused
  path does not call. The synthetic `_distinct_pk` name and the `&[0]` PK stay.
- **Effort:** S. **Value:** Medium — removes a second copy of the
  reindex-and-shard pipeline that can drift.
- **Verify:** `make e2e K='distinct'` and the set-op E2E suite — both views still
  compile and dedup correctly (multi-worker, to exercise the shard/exchange).

---

## Theme C — DML row-builder and seek-loop dedup

### C-DML-1 — Extract the merged-row builder (UPDATE SET ∥ DO UPDATE) and fix the lookup drift

Subsumes the per-row linear-scan perf drift into the extraction, so the lookup is
fixed once.

- **Files:** `dml/mutate.rs:92-117, 247-256`; `dml/insert.rs:313-363`.
- **Problem:** `write_set_columns` (mutate.rs:99-115) and the `Some(existing_batch)`
  arm of `client_side_merge_do_update` (insert.rs:341-358) duplicate the same
  merged-row builder: push pk + weight 1, seed `null_bits` from a source row's null
  word, then per payload column either (eval RHS → `null_word_set` on NULL +
  `append_column_value`) or carry the source column via `push_row_from`, then push
  `null_bits`. The null-on-NULL-assignment + carry-through semantics are covered by
  four `write_set_columns` tests; the insert.rs copy is untested. They have already
  drifted — mutate.rs does a per-column linear `assignments.iter().find` (O(cols²)),
  insert.rs pre-indexes `asn_by_col` for O(1). Critically, the PK source and the
  carry/null-seed source differ in DO UPDATE (PK from incoming `batch[i]`;
  nulls+columns from `existing_batch[0]`), so they must be separate parameters.
- **Change:** Extract a shared helper in mutate.rs taking the PK source separately
  from the carry/null-seed source plus a fallible per-column resolver closure:

  ```rust
  pub(crate) fn build_merged_row<F>(
      pk_src: &ZSetBatch, pk_idx: usize,        // PK pushed from here
      carry_src: &ZSetBatch, carry_idx: usize,  // null seed + carried columns from here
      schema: &Schema, dst: &mut ZSetBatch, mut resolve: F,
  ) -> Result<(), GnitzSqlError>
  where F: FnMut(usize) -> Result<Option<ColumnValue>, GnitzSqlError> {
      dst.pks.push_from(&pk_src.pks, pk_idx);
      dst.weights.push(1);
      let mut null_bits = carry_src.nulls[carry_idx];
      for (payload_idx, ci, col_def) in schema.payload_columns() {
          match resolve(ci)? {
              Some(cv) => {
                  null_word_set(&mut null_bits, payload_idx, matches!(cv, ColumnValue::Null));
                  append_column_value(&mut dst.columns[ci], cv, col_def.type_code)?;
              }
              None => {
                  let stride = col_def.type_code.wire_stride();
                  carry_src.columns[ci].push_row_from(carry_idx, stride, &mut dst.columns[ci]);
              }
          }
      }
      dst.nulls.push(null_bits);
      Ok(())
  }
  ```

  `write_set_columns` builds `asn_by_col: Vec<Option<&BoundExpr>>` once (sized to
  `actual_schema.columns.len()`), then calls `build_merged_row(current, i, current,
  i, schema, dst, |ci| asn_by_col[ci].map(|e| eval_set_expr(e, current, i, schema)).transpose())`
  — closing the O(cols²) drift in the same edit. The DO UPDATE arm calls
  `build_merged_row(batch, i, &existing_batch, 0, schema, &mut out, |ci|
  asn_by_col[ci].map(|rhs| eval_do_update_rhs(rhs, &existing_batch, batch, i, schema)).transpose())`.
  Export `build_merged_row` `pub(crate)`; insert.rs already imports from
  `crate::dml::mutate`.
- **Invariants:** Exact `payload_columns()` order; seed-then-flip-only-assigned null
  semantics; the distinct PK/carry sources in DO UPDATE. Z-set element identity =
  (PK, all payload) preserved. The four `write_set_columns` tests now exercise
  `build_merged_row` transitively.
- **Effort:** S. **Value:** Medium — removes a drift-prone untested copy and closes
  the O(cols²) lookup in one change.
- **Verify:** The four `write_set_columns` tests pass unchanged; add a DO-UPDATE
  merge test asserting null-on-NULL-assignment and carry-through against
  `existing_batch[0]` with PK from the incoming batch.

### C-DML-2 — Extract the first-hit index-seek loop (SELECT ∥ UPDATE/DELETE)

- **Files:** `dml/select.rs:92-108`; `dml/mutate.rs:171-179`; new helper in
  `exec/residual.rs`.
- **Problem:** `execute_select` and `resolve_where_rows` (mutate.rs:171-179) each
  hand-write the same equality-index seek loop over `collect_index_seek_candidates`
  output: per candidate try `client.seek_by_index`, `NoIndex ⇒ continue`, other
  `Err ⇒ Exec`, first `Ok ⇒ terminal` (a hit with zero rows still terminates). Only
  the success arm and the candidate-collection closure differ.
- **Change:** Add a generic helper in `exec/residual.rs` (where `ScanReply` lives;
  not dml/plan.rs, which is pure analysis with no client access):

  ```rust
  pub(crate) fn first_index_seek<T>(
      client: &mut GnitzClient,
      table_id: u64,
      candidates: Vec<crate::dml::plan::IndexSeekCandidate<'_>>,
      mut on_hit: impl FnMut(&mut GnitzClient, ScanReply, &[&Expr]) -> Result<T, GnitzSqlError>,
  ) -> Result<Option<T>, GnitzSqlError> {
      for (col_indices, key_vals, residual) in candidates {
          match client.seek_by_index(table_id, col_indices.as_slice(), &key_vals) {
              Ok(reply) => return Ok(Some(on_hit(client, reply, &residual)?)),
              Err(ClientError::NoIndex) => continue,
              Err(e) => return Err(GnitzSqlError::Exec(e)),
          }
      }
      Ok(None)
  }
  ```

  Pass `client` *into* `on_hit` so the closure doesn't conflict with the helper's
  `&mut client`. `select.rs` calls it with `|_c, reply, residual|
  residual_filtered(binder, &schema, reply, residual)`; `mutate.rs` with
  `|_c, (s, b, _), residual| resolve_residual_rows(binder, schema, s, b, residual)`.
  Each keeps its own candidate-collection step (select runs range candidates first).
- **Invariants:** Terminal semantics — `Some` for any served index (row count
  irrelevant), `None` only when every candidate returned `NoIndex`; select then
  falls to its non-indexed error, mutate to the predicate full scan.
- **Effort:** S. **Value:** Low — ~8 duplicated lines each, no correctness change.
- **Verify:** Both files compile; `make test`. No new test — behaviour unchanged.

---

## Theme D — Type safety

### D1 — Carry the join-key common type as `TypeCode`, not raw `u8`

- **Files:** `plan/view/predicates.rs:109-135, 227-240`; `plan/view/join.rs:30-39, 353`.
- **Problem:** The join-key common type T is carried as a bare `u8` even though it is
  a real `TypeCode`: `validate_join_key_pair` / `validate_range_join_key_pair` return
  `Result<u8>` (holding a `TypeCode`, doing `t as u8`), `RangeConjunct.tc` is `u8`,
  and the eq-tcs slot of `JoinPredicates` is `Vec<u8>`. The single consumer
  (join.rs:353) immediately reverses it with `TypeCode::from_validated_u8(t)`. Since
  `TypeCode` is `Copy`, this is a pointless round-trip discarding the enum's safety.
- **Change (entirely within gnitz-sql):** (1) The two `validate_*_join_key_pair`
  return `Result<TypeCode, _>` — drop the trailing `.map(|t| t as u8)`
  (`join_key_common_type` already returns `Option<TypeCode>`). (2) `RangeConjunct.tc:
  TypeCode`. (3) The eq-tcs slot of `JoinPredicates` → `Vec<TypeCode>` (and the
  `target_tcs`/`eq_tcs` locals). (4) At join.rs:353 use `t` directly instead of
  `TypeCode::from_validated_u8(t)`.
- **Invariants:** Do not touch the `0`="self-derive" sentinel, `carried_reindex_tc`,
  `resolve_reindex_type`, or `map_reindex(&[u8])`. That `0` sentinel is the
  gnitz-wire contract (collision-free: no `TypeCode` is 0) and keeps
  same-type/U128-vs-UUID/string circuits byte-identical at the wire boundary
  (join.rs:22-29). The `as u8` cast stays only at that `map_reindex`/`ExprBuilder`
  boundary.
- **Effort:** S. **Value:** Low — recovers `Copy`-enum safety on the one value that
  round-trips internally.
- **Verify:** Tests asserting `tcs == vec![TypeCode::X as u8]` become
  `vec![TypeCode::X]`; `make verify`.

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

### D3 — Add `ColumnDef::new` and drop the `fk_table_id: 0, fk_col_idx: 0` boilerplate

- **Files:** gnitz-core `ColumnDef` (add a method); ~20 gnitz-sql synthetic-column
  sites — `grep -rn 'fk_table_id: 0' crates/gnitz-sql/src` (join.rs, group_by.rs,
  set_op.rs, predicates.rs, project_schema.rs, ddl.rs:210-216).
- **Problem:** `gnitz_core::ColumnDef` is a bare 5-field struct with no constructor;
  every gnitz-sql synthetic-column construction spells out `fk_table_id: 0,
  fk_col_idx: 0` as dead boilerplate (the FK fields are only meaningful in the engine
  catalog path). A future field addition forces edits at every site.
- **Change:** Add an additive constructor in gnitz-core (no existing `impl
  ColumnDef`):

  ```rust
  impl ColumnDef {
      pub fn new(name: impl Into<String>, type_code: TypeCode, is_nullable: bool) -> Self {
          Self { name: name.into(), type_code, is_nullable, fk_table_id: 0, fk_col_idx: 0 }
      }
  }
  ```

  Replace the synthetic struct literals with `ColumnDef::new(...)`, preserving the
  exact name strings (`"_set_pk"`, `"_distinct_pk"`, `"_group_pk"`, `"m"`, etc.).
- **Invariants:** Synthetic column names are byte-stability-load-bearing for the
  shippable view (join.rs:349-357 comment); `new` takes the name verbatim. Leave the
  FK-bearing engine-side literals (`registry.rs`, catalog tests using `parent_tid`)
  as explicit struct literals — they set non-default FK fields. Do not add a
  `synthetic_u128_pk` wrapper — it would hide the exact name at the call site.
- **Effort:** S. **Value:** Low — removes ~20 lines of dead boilerplate;
  future-field-proof.
- **Verify:** `make verify`; view-shippability (synthetic-name byte-stability) tests
  unchanged.

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

### F2 — Fix the ConflictPlan doc (no target / no future-work framing)

- **Files:** `dml/insert.rs:23-37`
- **Problem:** The `enum ConflictPlan` doc (insert.rs:23-26) calls it a "Resolved ON
  CONFLICT target" holding the PK column or a "reserved slot for future ON CONSTRAINT
  / unique secondary targets (not implemented in v1)". The enum holds none of that —
  three target-free disposition variants (`Error`, `DoNothingPk`,
  `DoUpdatePk { assignments }`); the conflict target is validated and discarded in
  `validate_conflict_target` (called line 115) before the plan is built from `action`
  alone. The "reserved-slot / v1" framing is also forbidden future-work.
- **Change:** Replace with a doc describing the three resolved dispositions, no
  target/future framing:

  ```rust
  /// The resolved INSERT disposition after the ON CONFLICT clause (if any) is
  /// bound. The conflict target itself is validated and discarded in
  /// `validate_conflict_target`; only the action survives into the plan.
  enum ConflictPlan {
      /// Default INSERT: push with WireConflictMode::Error.
      Error,
      /// `ON CONFLICT [(pk)] DO NOTHING`: pre-filter existing PKs client-side via
      /// `seek`, then push survivors with WireConflictMode::Error.
      DoNothingPk,
      /// `ON CONFLICT (pk) DO UPDATE SET ...`: seek existing rows, merge the
      /// assignments client-side, then push merged with WireConflictMode::Update.
      DoUpdatePk { assignments: Vec<(usize, BoundUpdateExpr)> },
  }
  ```

- **Invariants:** None. The line-122 "in v1" *user-facing error string* is out of
  scope.
- **Effort:** S. **Value:** Low — removes a wrong type description and a forbidden
  future-work note.
- **Verify:** Read-through; no behavioural change.

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

## Theme G — Minor performance

### G1 — All-pass short-circuit in `apply_residual_filter`

- **Files:** `exec/residual.rs:56-77` (and `exec/batch.rs:8-16` `copy_batch_row`).
- **Problem:** `apply_residual_filter` copies each passing row via `copy_batch_row`,
  which for Strings/Bytes does `push(s[idx].clone())`. When the predicate passes
  every row (a broad-but-selective residual over a string/blob-heavy result), this
  clones every value — whereas the sibling `apply_projection` (batch.rs:200-214)
  deliberately bulk-moves whole column vectors to avoid exactly this.
- **Change:** Collect passing indices first; if every row passed, return the original
  batch unmoved (mirrors the empty-residual early return):

  ```rust
  let n = batch.pks.len();
  let mut matched: Vec<usize> = Vec::with_capacity(n);
  for i in 0..n {
      if row_passes_residuals(preds, &batch, i, actual_schema)? { matched.push(i); }
  }
  if matched.len() == n {
      return Ok((schema_opt, Some(batch)));   // all rows pass — no per-row copy
  }
  let mut new_batch = ZSetBatch::new(actual_schema);
  for &i in &matched {
      copy_batch_row(&batch, i, &mut new_batch, actual_schema);
  }
  Ok((schema_opt, Some(new_batch)))
  ```

  The selective path is unchanged except for materialising `matched` first (a cheap
  `usize` vec); the all-pass path skips the full row-by-row clone.
- **Invariants:** Returning the original `batch` when all rows pass is identical to a
  full copy (same rows, order, weights, schema). Keep the allocation-avoidance
  rationale shared with `apply_projection`.
- **Effort:** S. **Value:** Low — avoids a full string/blob clone on broad residual
  scans.
- **Verify:** `make test` (SELECT residual tests); results unchanged.

### G2 — Reserve `matched` capacity in `resolve_residual_rows`

- **Files:** `dml/mutate.rs:201-208`
- **Problem:** `matched` is `Vec::new()` then pushed one index at a time in a loop
  bounded by `batch.pks.len()` — a known tight upper bound.
- **Change:** `let mut matched =
  Vec::with_capacity(batch_opt.as_ref().map_or(0, |b| b.pks.len()));` (over-reserve is
  bounded by the fetched input and harmless).
- **Invariants:** Capacity hint only; no behavioural change.
- **Effort:** S. **Value:** Low — saves a few reallocations on WHERE-result
  reshaping. (Do not also reserve `new_batch`'s regions in `exec/residual.rs`:
  neither `PkColumn` nor `ColData` exposes a `reserve`, so it would require new
  cross-crate API for an already-amortised one-shot path.)
- **Verify:** `make test`; no test change.

---

## Sequencing

- **C1–C4** (correctness) first, independent of each other.
- **B2** adds `compile_bound_expr_to_program` and narrows `compile_bound_expr`'s
  return; **B1**'s `build_optional_filter` uses it — land B2 then B1.
- **B4** depends on nothing else but exercises the shard path; run its E2E
  multi-worker.
- **D1** (TypeCode) and **D3** (`ColumnDef::new`) both touch join.rs:349-357 — land D1
  first so the column construction uses the typed `t` before D3 wraps it in
  `ColumnDef::new`.
- **D2** (coordinate newtypes) is the largest item; land it after D1 so the join-key
  type plumbing is already simplified.
- **F4** must follow **B1** and **C-DML-1** (both call `binder.bind_expr`), or update
  those calls in the F4 edit.

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
