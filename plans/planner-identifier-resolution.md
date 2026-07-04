# SQL planner identifier-resolution correctness

Two independent identifier-handling defects, sequenced because the second builds
on the first:

1. **Relation-name resolution is case-sensitive**, while columns, join aliases,
   and function names are case-insensitive — an inconsistency that also breaks
   plain `CREATE TABLE Foo; SELECT * FROM foo` and case-varying CTE references.
2. **Auto-generated index names collide** when column names contain `_`, because
   the name joins columns with a `_` that is itself a legal identifier character.

## Part 1 — Case-insensitive relation-name resolution

### Problem

SQL identifiers are case-insensitive, and GnitzDB already treats them so for
**columns** (`find_unique_column` uses `eq_ignore_ascii_case`), **join/derived
aliases** (`build_alias_map` lowercases keys, `resolve_qualified_column`
lowercases the probe), and **function names**. But every **schema / table / view
/ index name** is stored verbatim and compared with exact byte equality — there
is no case fold at any store or lookup site, client-side or engine-side. Two
consequences:

- `CREATE TABLE Foo` then `SELECT * FROM foo` fails: `find_table_record`
  (`client.rs`) does `col_str(...) != Some(table_name)` against the stored `Foo`.
- `WITH Cc AS (…) SELECT * FROM cc` fails or mis-resolves: the CTE is cached in
  the binder under raw key `"Cc"` (`cache_alias`, `dispatch.rs`), the final body
  probes `self.cache.get("cc")` (case-sensitive `HashMap`) → miss → falls to a
  catalog lookup of `cc`, which either errors ("not found") or silently binds an
  unrelated base table named `cc` — the CTE should shadow it (standard SQL).

The self-referencing FK check is *already* case-insensitive
(`ref_table.eq_ignore_ascii_case(current_table_name)`, `ddl.rs`) while the
cross-table FK resolution it sits next to is case-sensitive — the same latent
asymmetry.

### Design

Make lowercase the **canonical stored form** of every user relation/schema/index
name, folded at the two boundaries every catalog name flows through: the
**client** (the single catalog gateway) and the **binder cache** (which also
holds CTE/derived aliases that never reach the catalog). Because
`validate_user_identifier` constrains these names to ASCII `[A-Za-z0-9_]` and
forbids a leading `_`, `to_ascii_lowercase()` is a total, collision-free fold
(no Unicode surprises; a lowercased name still cannot start with `_`, so the
reserved-prefix rule is unaffected).

Canonical storage (rather than store-raw + compare-insensitively) is chosen
deliberately:

- The DROP paths cancel a `+1` catalog row by re-emitting a `-1` row whose name
  column must **byte-match** the stored `+1`. With canonical lowercase storage,
  lowercasing the incoming drop name reproduces the stored bytes exactly.
  (Compare-insensitively would let `DROP … foo` emit a `-1` keyed `foo` that
  fails to cancel a stored `Foo`.)
- The uniqueness prechecks (`precheck_qname_unique`, schema/index dup checks)
  become case-insensitive for free: canonical inputs make `CREATE TABLE Foo` and
  `CREATE TABLE foo` collide on the same stored `foo`, with no change to those
  checks.
- The engine needs **no production change**: it receives already-canonical names
  from the client, so its name caches (`schema_by_name`, `entity_by_qname`,
  `index_by_name`) key on lowercase and its prechecks compare lowercase vs
  lowercase. Engine-derived names (`make_fk_index_name`) auto-derive from the
  stored lowercase table/schema names.

The user-visible cost is that stored names display lowercased (as in PostgreSQL's
unquoted-identifier folding) — acceptable pre-alpha.

#### 1a. Client — canonicalize at store and probe (`crates/gnitz-core/src/client.rs`)

Add one private helper and apply it at every name store and every name probe.

```rust
/// Canonical stored form of a user relation/schema/index name. Names are ASCII
/// `[A-Za-z0-9_]` (enforced by `validate_user_identifier`), so an ASCII lowercase
/// is a total, collision-free fold and the single definition of catalog-name
/// case-insensitivity.
fn canon_name(name: &str) -> String {
    name.to_ascii_lowercase()
}
```

**Store sites** — lowercase the name before it enters the batch:

- `create_schema` — `.str_val(name)` → `.str_val(&canon_name(name))`.
- `create_table` — the `.str_val(table_name)` payload, and each inline
  `spec.name` index-name payload.
- `create_view_chain` — the `ViewRecord { name: pv.name, … }` (canonicalize
  `pv.name` when building the record, before `append_view_row`).
- `create_index` — the `.str_val(index_name)` payload **and** its duplicate
  pre-check comparison.

**Probe sites** — lowercase the incoming name before the scan/compare (and, for
schema-id resolution, canonicalize `schema_name` before `find_schema_id`):

- `find_schema_id`, `find_table_record`, `find_view_record` — compare
  `col_str(...) == Some(&canon_name(name))` (canonicalize once at each public
  entry: `resolve_table_id`, `resolve_table_or_view_id`, and every `drop_*`).
- `drop_schema` — canonicalize `name` at entry; the `-1` re-emit then reproduces
  the stored bytes.
- `drop_table` / `drop_view` — canonicalize the probe; the retraction reuses the
  **stored** `record.name` (already canonical), so it round-trips unchanged.
- `drop_index_by_name` — canonicalize `index_name` at entry so both the
  `name != index_name` compare **and** the `-1` re-emit (`.str_val(index_name)`)
  use the canonical bytes.

The cleanest shape is to canonicalize once at each public method's entry
(`let name = canon_name(name);` shadowing the parameter), so the private
`find_*` scanners receive already-canonical probes and every downstream compare
and re-emit is consistent.

#### 1b. Binder cache — lowercase keys (`crates/gnitz-sql/src/bind/resolve.rs`)

The binder cache holds base-table resolutions **and** CTE/derived-table aliases
(the latter never reach the catalog). Key it case-insensitively, matching the
join `AliasMap` convention (`build_alias_map` already lowercases):

```rust
    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        let key = name.to_ascii_lowercase();
        if let Some(entry) = self.cache.get(&key) {
            return Ok((entry.table_id, Rc::clone(&entry.schema)));
        }
        // (reserved-prefix guard from the binder-guard plan sits here)
        let (tid, schema) = client
            .resolve_table_or_view_id(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        let rc = Rc::new(schema);
        self.cache_relation(key, tid, Rc::clone(&rc));
        Ok((tid, rc))
    }
```

`cache_relation` then stores the already-lowercased `key` verbatim.
`resolve_base_table` lowercases its cache key the same way. `cache_alias` (the
CTE/derived-table entry point, called from `dispatch.rs`) lowercases its key:

```rust
    pub(crate) fn cache_alias(&mut self, name: String, resolved: (u64, Rc<Schema>)) {
        self.cache_relation(name.to_ascii_lowercase(), resolved.0, resolved.1);
    }
```

With this, `WITH Cc AS (…) SELECT … FROM cc` caches under `cc` and the `cc`
probe hits it; a base-table reference in any case lowercases to the same key and,
on a miss, probes the now case-insensitive client.

#### 1c. One case-fold primitive for identifiers (`resolve.rs`, `validate.rs`, `join.rs`, `dispatch.rs`)

Part 1 makes `to_ascii_lowercase()` the canonical identifier fold. Several
resolution sites still fold with the Unicode `str::to_lowercase()`: the
join/derived alias-map keys and probe (`build_alias_map`,
`resolve_qualified_column`), the join liveness/provenance maps
(`plan/view/join.rs` — alias keys and column-name sets), duplicate-column
detection (`reject_duplicate_column_names`), and the operator-name collector
(`collect_operator_names`). Every one folds a name that `validate_user_identifier`
already constrains to ASCII `[A-Za-z0-9_]`, so over the supported input space
`to_lowercase()` and `to_ascii_lowercase()` return the same bytes — and each fold
is applied symmetrically to both the stored key and the probe, so switching them
cannot desynchronize a key/probe pair. Change every one to `to_ascii_lowercase()`
so name resolution uses a single fold, consistent with the canonical rule. This
is a consistency change, not a measured optimization; make no perf claim about it.

Sites: `bind/resolve.rs` (`build_alias_map`, `resolve_qualified_column`);
`plan/validate.rs` (`reject_duplicate_column_names`); `plan/view/join.rs` (the
liveness `insert`/`extend`/`contains` folds and the `alias_order` / provenance
alias-map keys); `plan/view/dispatch.rs` (`collect_operator_names`).

### Part 1 tests

Rust (`integration`):

1. `relation_names_are_case_insensitive` (`planner_create_table.rs`): `CREATE
   TABLE Foo (id BIGINT PRIMARY KEY)`, then `SELECT * FROM foo` and `INSERT INTO
   FOO …` both resolve; `CREATE TABLE foo (…)` is rejected as a duplicate.
2. `cte_reference_case_insensitive` (`planner_cte.rs`): `CREATE VIEW v AS WITH Cc
   AS (SELECT id FROM t WHERE id > 0) SELECT id FROM cc` compiles, and — with a
   base table `cc` also present — resolves `cc` to the CTE, not the base table.
3. `drop_matches_stored_name_any_case` (`planner_create_table.rs`): create
   `Bar`, `DROP TABLE bAr` succeeds and the row is gone (the `-1` cancels the
   stored `bar`).

Existing tests are unaffected: they create and reference relations in one
consistent (lowercase) case, and lowercasing a lowercase name is the identity.

## Part 2 — Collision-free auto-generated index names

### Problem

`default_index_name` (`crates/gnitz-sql/src/plan/validate.rs`) is
`{schema}__{table}__idx_{col_names.join("_")}`. Column names may themselves
contain `_`, so two unnamed composite indexes on distinct column sets can
generate the same string — e.g. `(a, b_c)` and `(a_b, c)` both render
`…__idx_a_b_c`. The second `CREATE` then fails the duplicate-name guard
(`client.rs` pre-check / engine `precheck_sys_ingest` "Index already exists") —
a confusing rejection of two structurally distinct indexes.

### Design

Append the smallest free numeric suffix **only on collision** (PostgreSQL's
scheme), so the readable base name is kept for the overwhelmingly common
non-colliding case (and every currently-pinned `…__idx_col` / `…__idx_a_b` name
is unchanged), while colliding auto-names get `…__idx_a_b_c`, `…__idx_a_b_c_2`, …
Disambiguation runs in the planner, where the auto-vs-explicit distinction is
known; an explicit duplicate name still errors, unchanged. Names are compared on
their canonical (lowercase) form, so Part 1 must land first.

**Disambiguation must not resurrect a true duplicate.** Today the *only* thing
rejecting a second auto-named `CREATE INDEX ON t(a)` is the name collision
(`client.create_index`'s IDX_TAB name scan / the engine `precheck_sys_ingest`
mirror) — there is no structural (column-set) index dedup anywhere. Left
unguarded, the disambiguator would turn that rejection into a silent second index
`…__idx_a_2` on the identical column set. So the planner first rejects a re-index
of an **identical column set already carrying the auto base name**, and only then
disambiguates. The discriminator is `(name == base) ∧ (cols == col_indices)`, read
off the raw IDX_TAB rows:

- **True duplicate** (`(a)` then `(a)`): the prior index stored the base name over
  the same columns → reject, exactly as before.
- **False collision** (`(a, b_c)` then `(a_b, c)`): the prior index has the base
  name but a *different* column set → disambiguate to `…__idx_a_b_c_2`.
- **A same-column index under a different name** — an FK-backing index
  (`…__fk_…`) or an explicitly-named user index — never carries the auto base
  name, so it neither blocks the auto-name nor is disturbed. This is why the check
  keys on the raw `(name, cols)` rows rather than the deduped `table_indexes`
  circuit list, whose entries fold every name on a column set into one row (a
  cols-only test there would spuriously reject `CREATE INDEX ON t(fk_col)` and
  block promoting an FK index to `UNIQUE`).

#### 2a. Client — read-only index (name, columns) enumeration (`crates/gnitz-core/src/client.rs`)

The catalog exposes no index list to the planner today; the name readers inline-scan
IDX_TAB. Add one small read-only helper returning each live IDX_TAB row's `(name,
cols)` — name from column 4, the packed source columns from column 3 (the same
slots `create_index` writes and `drop_index_by_name` reads). The disambiguator
needs the columns (for the true-duplicate check) and the names (for the taken
set), so one scan serves both. The stored name is already canonical (Part 1
canonicalizes at store time), matching the canonical `base`:

```rust
/// `(name, indexed columns)` of every live secondary-index IDX_TAB row (name in
/// canonical lowercase). The planner uses this to reject a re-index of an
/// identical column set under the auto base name and to disambiguate an
/// auto-generated name against the taken set.
pub fn index_name_cols(&mut self) -> Result<Vec<(String, gnitz_wire::PkColList)>, ClientError> {
    let (_, idx_batch, _) = self.conn.scan(IDX_TAB, &mut self.schema_cache)?;
    let Some(idx_batch) = idx_batch else { return Ok(Vec::new()) };
    let mut out = Vec::new();
    for i in idx_batch.live_rows() {
        let Some(name) = col_str(&idx_batch.columns[4], i)? else { continue };
        let cols = gnitz_wire::unpack_pk_cols(col_u64(&idx_batch.columns[3], i)?);
        out.push((name.to_string(), cols));
    }
    Ok(out)
}
```

The auto base name embeds `{schema}__{table}`, so it can only ever collide with
another index on the same table; a global scan is therefore correct for both the
duplicate check and the taken set (a different table's row can share neither the
name nor, meaningfully, the column-set-plus-name pair).

#### 2b. Planner — disambiguate auto names (`crates/gnitz-sql/src/plan/validate.rs`, `ddl.rs`)

A shared helper picks the first free name against a taken-set:

```rust
/// Return `base` if free, else the first `{base}_{n}` (n ≥ 2) not in `taken`.
/// `taken` holds canonical (lowercase) names; `base` is already canonical. Only
/// auto-generated names are routed here — an explicit collision still errors.
pub(crate) fn disambiguate_index_name(base: String, taken: &std::collections::HashSet<String>) -> String {
    if !taken.contains(&base) {
        return base;
    }
    for n in 2.. {
        let candidate = format!("{base}_{n}");
        if !taken.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!("u32 range exhausted")
}
```

`default_index_name` lowercases its output so the base is canonical:

```rust
pub(crate) fn default_index_name(schema_name: &str, table_name: &str, col_names: &[&str]) -> String {
    format!("{schema_name}__{table_name}__idx_{}", col_names.join("_")).to_ascii_lowercase()
}
```

- **Standalone `CREATE INDEX`** (`execute_create_index`, `ddl.rs`): only the
  auto-name branch runs the duplicate check and disambiguation, against the
  existing catalog rows:

  ```rust
  let index_name = match explicit_name {
      Some(name) => name, // explicit: an exact-name collision still errors in `create_index`
      None => {
          let names: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();
          let base = default_index_name(schema_name, &table_name, &names);
          let existing = client.index_name_cols().map_err(GnitzSqlError::Exec)?;
          // A prior *auto-named* index on this exact column set is the same index:
          // reject it, preserving the pre-disambiguation duplicate rejection. An
          // FK-backing or explicitly-named index on these columns carries a
          // different name, so it never blocks a distinct auto-name.
          if existing
              .iter()
              .any(|(name, cols)| name == &base && cols.as_slice() == col_indices.as_slice())
          {
              return Err(GnitzSqlError::Plan(format!(
                  "an index on these columns already exists as '{base}'"
              )));
          }
          let taken: std::collections::HashSet<String> = existing.into_iter().map(|(n, _)| n).collect();
          disambiguate_index_name(base, &taken)
      }
  };
  ```

- **Inline `UNIQUE` in `CREATE TABLE`** (`execute_create_table`, `ddl.rs`): the
  table is new, so no pre-existing index can collide (index names embed the
  table name, unique per schema); only the auto-names **within this bundle** can
  collide with each other. Seed `taken` with the bundle's explicit constraint
  names, then for each auto-name reject an identical column set already assigned an
  auto-name (the bundle-local true duplicate, e.g. `UNIQUE(a), UNIQUE(a)`) and
  otherwise disambiguate:

  ```rust
  let mut taken: std::collections::HashSet<String> = std::collections::HashSet::new();
  // Explicit constraint names first — fixed points auto-names route around.
  for (_, constraint_name) in &unique_cols {
      if let Some(n) = constraint_name {
          taken.insert(n.to_ascii_lowercase());
      }
  }
  // Auto-names assigned so far, with their column sets: an identical set is a
  // true duplicate (reject); a distinct set rendering the same base disambiguates.
  let mut auto: Vec<(String, &[u32])> = Vec::new();
  let mut index_names: Vec<String> = Vec::with_capacity(unique_cols.len());
  for (col_indices, constraint_name) in &unique_cols {
      let name = match constraint_name {
          Some(n) => n.clone(),
          None => {
              let col_names: Vec<&str> =
                  col_indices.iter().map(|&c| cols[c as usize].name.as_str()).collect();
              let base = default_index_name(schema_name, &table_name, &col_names);
              if auto.iter().any(|(b, c)| b == &base && *c == col_indices.as_slice()) {
                  return Err(GnitzSqlError::Plan(format!(
                      "duplicate UNIQUE constraint on the same columns ('{base}')"
                  )));
              }
              let name = disambiguate_index_name(base.clone(), &taken);
              auto.push((base, col_indices.as_slice()));
              taken.insert(name.clone());
              name
          }
      };
      index_names.push(name);
  }
  ```

The client's store-time canonicalization (Part 1) lowercases whatever name it
receives, and these are already lowercase, so store and disambiguation agree.

### Part 2 tests

Rust (`integration`):

1. `unnamed_index_name_collision_disambiguated` (`planner_create_index.rs`):
   create a table with columns `a`, `b_c`, `a_b`, `c`; `CREATE INDEX ON t(a, b_c)`
   then `CREATE INDEX ON t(a_b, c)` — both succeed; assert `DROP INDEX
   {sn}__t__idx_a_b_c` and `DROP INDEX {sn}__t__idx_a_b_c_2` each succeed (both
   indexes exist under distinct names).
2. `inline_unique_name_collision_disambiguated` (`planner_create_table.rs`):
   `CREATE TABLE t (…, UNIQUE(a, b_c), UNIQUE(a_b, c))` succeeds; both indexes
   are droppable by their (suffixed) auto-names.
3. `same_columns_auto_index_rejected` (`planner_create_index.rs`): `CREATE INDEX
   ON t(a)` then `CREATE INDEX ON t(a)` — the second is rejected by the
   same-columns check (the identical column set already carries the auto base
   name), *not* silently disambiguated to `…__idx_a_2`.
4. `same_columns_named_index_coexists` (`planner_create_index.rs`): `CREATE INDEX
   my_idx ON t(a)` then `CREATE INDEX ON t(a)` — both succeed (the named index does
   not carry the auto base name, so it does not block the auto-name), pinning that
   the reject keys on `(name == base) ∧ (cols == col_indices)`, not on columns
   alone.
5. `inline_duplicate_unique_rejected` (`planner_create_table.rs`): `CREATE TABLE t
   (a BIGINT PRIMARY KEY, UNIQUE(a), UNIQUE(a))` is rejected as a duplicate UNIQUE
   constraint on the same columns.

Test-contract updates (the previous behavior was the bug):

- `create_index_duplicate_name_is_rejected` (`planner_create_index.rs`): today
  asserts the *second auto-named* `CREATE INDEX ON t(v)` is rejected. It still is,
  now by the same-columns structural check rather than the downstream name
  collision (both indexes are the identical column set). Verify the rejection
  still fires and adjust only the expected error text if it changed.
- The engine mirror `test_create_index_duplicate_rejected`
  (`catalog/tests/index_tests.rs`) is unaffected — it drives the engine's direct
  API with an explicit duplicate name.

Unchanged pins: `…__idx_col`, `…__idx_a_b` DROP-by-name tests
(`planner_create_table.rs`) keep their exact names — non-colliding auto-names are
never suffixed.

## Files touched

- `crates/gnitz-core/src/client.rs` — `canon_name` + store/probe canonicalization
  (Part 1); `index_name_cols` (Part 2).
- `crates/gnitz-sql/src/bind/resolve.rs` — lowercase binder-cache keys (Part 1);
  `to_ascii_lowercase` in `build_alias_map` / `resolve_qualified_column` (Part 1c).
- `crates/gnitz-sql/src/plan/validate.rs` — `default_index_name` lowercase +
  `disambiguate_index_name` (Part 2); `to_ascii_lowercase` in
  `reject_duplicate_column_names` (Part 1c).
- `crates/gnitz-sql/src/plan/view/join.rs` — `to_ascii_lowercase` in the liveness
  and provenance alias/column folds (Part 1c).
- `crates/gnitz-sql/src/plan/view/dispatch.rs` — `to_ascii_lowercase` in
  `collect_operator_names` (Part 1c).
- `crates/gnitz-sql/src/plan/ddl.rs` — auto-name duplicate check + disambiguation
  in `execute_create_index` and `execute_create_table` (Part 2).
- Tests: `planner_create_table.rs`, `planner_cte.rs`, `planner_create_index.rs`.

## Sequencing

- [ ] 1. Part 1 (client canonicalization + binder-cache keys) + Part 1c
      (`to_ascii_lowercase` sweep); Part 1 tests; `make verify` green.
- [ ] 2. Part 2 (`index_name_cols` + same-columns duplicate check +
      disambiguation, both DDL paths); Part 2 tests + contract updates;
      `make verify` green.
- [ ] 3. `make e2e` green (W=4) — exercises the DDL/resolution paths end-to-end.
