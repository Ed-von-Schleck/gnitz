# Derived tables must not see their FROM siblings

## Problem

A subquery in FROM (a *derived table*) is not correlated unless it is `LATERAL`,
which GnitzDB rejects. So a non-LATERAL derived table's body may reference CTEs
and catalog relations, but **not** another item in the same FROM clause. GnitzDB
violates this: it resolves a sibling derived table's alias, giving unmarked
LATERAL-like scoping over a relation the join circuit evaluates as a whole (not
per-row), i.e. silently wrong semantics for exactly the shape the engine declines
to support.

The leak is in `compile_derived_tables` (`crates/gnitz-sql/src/plan/view/dispatch.rs`).
It walks the top-level FROM (base relation + each join's relation) and compiles
each derived table with `compile_hidden_body`, which **registers the alias in the
shared `binder` immediately** (`binder.cache_alias(register_name, …)` at the tail
of `compile_hidden_body`). The `binder` is threaded by `&mut` through the whole
loop, so a later sibling's body resolves the earlier alias:

- `compile_hidden_body` for a plain filter/map body lowers via
  `lower_linear`, whose `binder.resolve(client, &table_name)` returns a cache hit
  on the earlier sibling (`plan/lp.rs`, `bind/resolve.rs` `resolve` → `self.cache.get(name)`).

So in `FROM (SELECT …) a JOIN (SELECT … FROM a) b ON …`, compiling `b`'s body
resolves `a` to sibling `a` instead of a catalog relation `a` (or an
out-of-scope error). Two failure modes:

- **No catalog relation of that name** — the reference should error ("relation
  not found"); instead it silently binds the sibling.
- **A catalog relation of that name exists** — the reference should bind the
  catalog relation (standard SQL: siblings are out of scope); instead the sibling
  shadows it, changing results.

**Reachability is via `JOIN … ON` only.** A comma / `CROSS JOIN` of two derived
tables never reaches join emission — `join_on_and_type` accepts only
`INNER/LEFT/RIGHT/FULL … ON` and errors on any constraint-free join
(`plan/view/join.rs`) — so the leak surfaces only when the two derived tables are
joined with an `ON` clause and the second's body names the first's alias.

CTE scoping is correct today and must stay so: `inline_ctes` runs before
`compile_derived_tables`, compiling each CTE and registering it immediately, so a
later CTE can reference an earlier one (`WITH x AS (…), y AS (SELECT … FROM x)`)
and every CTE is visible inside a derived table's body. Only *sibling
derived-table* visibility is wrong.

## Design

Compile each derived table isolated from its siblings, and register the sibling
aliases into the binder only **after** the whole FROM is compiled. CTEs and base
relations stay visible throughout (they are never removed); a sibling reference
falls through to the catalog, resolving a real relation or erroring — the correct
non-LATERAL behavior.

The isolation is per-alias and local to `compile_derived_tables` — nothing in
`compile_hidden_body` or the CTE path changes, so CTE-sees-earlier-CTE is
untouched. For each derived table: snapshot any binding its alias currently
shadows (a same-named CTE), let `compile_hidden_body` register the sibling, then
lift that registration back out and restore the shadowed binding for the rest of
the loop. After the loop, register every sibling alias so the final body resolves
them (a sibling alias shadows a same-named CTE in the final body, as SQL requires).

### Binder — peek and remove a cache entry (`crates/gnitz-sql/src/bind/resolve.rs`)

Two small accessors over the existing cache, keyed identically to
`cache_relation` / `resolve` (the single key convention for this cache):

```rust
    /// The cached resolution for `name`, if present, without removing it —
    /// mirrors `resolve`'s cache-hit branch.
    pub(crate) fn peek_alias(&self, name: &str) -> Option<(u64, Rc<Schema>)> {
        self.cache.get(name).map(|e| (e.table_id, Rc::clone(&e.schema)))
    }

    /// Remove and return `name`'s cached resolution, if present.
    pub(crate) fn remove_alias(&mut self, name: &str) -> Option<(u64, Rc<Schema>)> {
        self.cache.remove(name).map(|e| (e.table_id, e.schema))
    }
```

### `compile_derived_tables` — isolate, then defer registration (`dispatch.rs`)

The loop body is unchanged through the envelope validation; the tail changes from
one `compile_hidden_body` call to compile-isolated + defer:

```rust
    let from = &select.from[0];
    // A non-LATERAL derived table may reference CTEs and catalog relations but not
    // a sibling FROM item. Compile each isolated from its siblings and register the
    // aliases only after the whole FROM is compiled, so a later sibling naming an
    // earlier one resolves to a catalog relation (or errors) — never the sibling.
    let mut deferred: Vec<(String, (u64, Rc<Schema>))> = Vec::new();
    for tf in std::iter::once(&from.relation).chain(from.joins.iter().map(|j| &j.relation)) {
        let TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } = tf
        else {
            continue;
        };
        let Some(alias) = alias else {
            return Err(GnitzSqlError::Unsupported(
                "a derived table (subquery in FROM) needs an alias".to_string(),
            ));
        };
        let ctx = format!("derived table '{}'", alias.name.value);
        if *lateral {
            return Err(GnitzSqlError::Unsupported(format!("{ctx}: LATERAL is not supported")));
        }
        reject_unhonored_query_clauses(
            subquery,
            HonoredQueryClauses { with: false, limit: false },
            &ctx,
        )?;
        let sub_select = match subquery.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => return Err(GnitzSqlError::Unsupported(format!("{ctx}: only a SELECT subquery is supported"))),
        };
        if sub_select.from.len() != 1 || sub_select.distinct.is_some() {
            return Err(GnitzSqlError::Unsupported(format!(
                "{ctx}: only a single-FROM, non-DISTINCT subquery is supported"
            )));
        }
        // Any binding this alias currently shadows (a same-named CTE): keep it
        // visible to later siblings while this sibling's registration is held back.
        let shadowed = binder.peek_alias(&alias.name.value);
        compile_hidden_body(client, binder, sub_select, &alias.name.value, &alias.columns, &ctx, chain)?;
        // `compile_hidden_body` registered the alias; lift it out so siblings can't
        // see it, restoring the shadowed binding for the rest of the FROM.
        let resolved = binder
            .remove_alias(&alias.name.value)
            .expect("compile_hidden_body registered the derived-table alias");
        if let Some(prev) = shadowed {
            binder.cache_alias(alias.name.value.clone(), prev);
        }
        deferred.push((alias.name.value.clone(), resolved));
    }
    // The final body resolves every sibling; a sibling alias shadows a same-named CTE.
    for (name, resolved) in deferred {
        binder.cache_alias(name, resolved);
    }
    Ok(())
```

Load-bearing facts, verified against current source:

- **`compile_hidden_body` always registers the alias.** Its final statement is an
  unconditional `binder.cache_alias(register_name.to_string(), (hidden_vid, schema))`,
  for every body shape (join / group-by / linear), so the `remove_alias(…).expect(…)`
  can never miss.
- **The `chain` is untouched by the binder isolation.** `compile_hidden_body`
  pushes its segment onto `chain` regardless of the binder; segments accumulate
  correctly whether or not the alias is later lifted out.
- **A derived table can still shadow a same-named CTE inside its own body.**
  `compile_hidden_body` registers the alias only *after* compiling the body, so a
  derived table `a` whose body reads `FROM a` still resolves that inner `a` to the
  CTE `a` (the shadowing binding, captured in `shadowed`, is present throughout the
  body compilation and restored afterward).
- **Ordering into the final body holds.** `compile_derived_tables` registers the
  deferred aliases before returning; the caller then compiles the main body
  (`plan_join_chain` resolves each derived table by its alias via `binder.resolve`),
  so every sibling is in the cache when the join is planned.

## Tests

Rust (`integration`, `crates/gnitz-sql/tests/planner_cte.rs` — same
`compile_hidden_body` machinery):

1. `sibling_derived_table_out_of_scope`: base table `t(id BIGINT PRIMARY KEY)`,
   no relation named `a`. `CREATE VIEW v AS SELECT b.id FROM (SELECT id FROM t) a
   JOIN (SELECT id FROM a) b ON a.id = b.id` — now **errors** ("relation … not
   found" for `a`): the sibling is out of scope and no catalog `a` exists. (Before
   the fix this compiled, binding `b`'s `a` to the sibling.)
2. `sibling_alias_binds_catalog_relation`: base tables `t(id BIGINT PRIMARY KEY)`
   and `a(id BIGINT PRIMARY KEY, v BIGINT)`. `CREATE VIEW v AS SELECT b.v FROM
   (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id` — compiles;
   `b`'s `FROM a` binds the **catalog** table `a` (which has `v`), not the sibling
   derived `a` (which has only `id`). Pins that a sibling reference resolves through
   the catalog.
3. `sibling_reference_binds_cte_not_sibling`: base table `t(id BIGINT PRIMARY KEY,
   v BIGINT)`. `CREATE VIEW v AS WITH a AS (SELECT id, v FROM t WHERE v > 0) SELECT
   b.v FROM (SELECT id FROM t) a JOIN (SELECT id, v FROM a) b ON a.id = b.id` —
   compiles; `b`'s `FROM a` binds the **CTE** `a` (has `v`), which the same-named
   derived table `a` shadows only in the final body. Pins the shadow snapshot /
   restore (without it, `b`'s `a` would find no `a` and error).

Python E2E (`crates/gnitz-py/tests/test_derived_table.py`):

4. `test_sibling_derived_table_not_correlated`: same shape as test 2 end-to-end —
   create the two base tables and the view, insert rows, and assert the view's rows
   reflect binding `b` to catalog `a` (the sibling is not correlated). Run under
   `GNITZ_WORKERS=4`.

Existing coverage that must stay green: the CTE chain tests (`planner_cte.rs`,
`test_cte_chain.py` — CTE-references-earlier-CTE is unchanged) and the derived-table
tests (`test_derived_table.py` — a single derived table, or siblings that reference
only base relations, are unaffected).

## Files touched

- `crates/gnitz-sql/src/bind/resolve.rs` — `peek_alias` / `remove_alias`.
- `crates/gnitz-sql/src/plan/view/dispatch.rs` — sibling isolation + deferred
  registration in `compile_derived_tables`.
- Tests: `crates/gnitz-sql/tests/planner_cte.rs`,
  `crates/gnitz-py/tests/test_derived_table.py`.

## Sequencing

- [ ] 1. `peek_alias` / `remove_alias`; sibling isolation in
      `compile_derived_tables`; Rust tests 1–3; `make verify` green.
- [ ] 2. `make e2e` green (W=4), test 4 included.
