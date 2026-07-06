# Hidden key slots: a view's visible schema is what the SELECT projects

## Problem

Every SQL view emitter splices its physical key columns into the user-visible
schema as leading columns. Where the key is natural (source PK projected by
the user, GROUP BY on a natural key) this is invisible; where it is synthetic
it leaks fabricated columns to the client:

| Column(s) | Emitter | Site |
|---|---|---|
| `_join_pk{,_i}` | equi join | `join.rs:775-784` |
| `_pair_pk_{slot}` | range/band join | `join.rs:1111-1124` |
| `_join_pk{,_i}` / `_src_pk_{i}` | EXISTS/IN | `exists.rs:481-490`, `:658-673` |
| `_set_pk` | UNION/INTERSECT/EXCEPT | `set_op.rs:363-370` |
| `_distinct_pk` | SELECT DISTINCT | `set_op.rs:~408-424` |
| `_group_pk` | GROUP BY fallback | `group_by.rs:378-384` |

and, the natural-side leak: `place_pk_front`
(`gnitz-sql/src/codec/project_schema.rs:74-96`) auto-prepends *unprojected*
source-PK columns to every simple view and reorders the user's projection to
put PK columns first.

Consequences, each with tests exercising it today: `SELECT *` returns nonsense
hash columns (`test_aggregates.py` asserts `_group_pk` present); wildcard
expansion re-admits upstream synthetic PKs into payload, forcing per-shape
shielding (`live_columns_projection`'s PK-drop half, `join.rs:266-320`; the
set-op wildcard at `set_op.rs:132-188` even hashes an upstream `_join_pk` into
UNION/DISTINCT row identity, polluting dedup semantics); `_src_pk` fresh-naming
exists only to dodge the duplicate-name guard (`exists.rs:658-673`); positional
column aliases over hidden JOIN segments are rejected because "its leading
synthetic-PK region is invisible" (`dispatch.rs:302-331`); the Python test
oracle relies on every caller passing an explicit `project` allow-list so
synthetic PKs are never read (`tests/_oracle.py:298-315`).

## Load-bearing facts

- The engine is nameless and positional: `SchemaColumn`
  (`gnitz-engine/src/schema.rs:27-32`) is `{type_code, size, nullable,
  is_signed}`; compiler, VM, exchange, reindex, shard cols, and provenance
  offsets all index the full physical schema and never a name. Visibility
  metadata therefore changes **nothing** in the engine's execution layers.
- PK-ness is already a side list: `Schema { columns, pk_cols }`
  (`gnitz-core/src/protocol/types.rs:57-62`), `META_FLAG_IS_PK` +
  `META_FLAG_PK_POS_*` per-column flags (`gnitz-wire/src/flags.rs:201-211`),
  VIEW_TAB `pk_col_idx`. Arbitrary PK positions are supported end-to-end.
- A bound key slot's byte offset in the PK region depends on the widths of
  *preceding* slots (`Schema::pk_byte_offset`, `types.rs:124-132`), so key
  slots must stay entries in the one physical column list — visibility is a
  flag on a column, not a separate descriptor.
- COL_TAB already round-trips a per-column marker with no engine meaning:
  `is_serial` (`client.rs:1397-1408` write, `:1241-1257` read). `is_hidden`
  follows that precedent, plus a wire meta-schema flag (`is_serial` has none).
- Views are never DML targets, carry no `unique_pk`, no secondary indexes, no
  ORDER BY; a view key's only roles are routing, sort/consolidation identity,
  and byte-based seek — all blind to visibility.

## Committed design

One new per-column attribute, `is_hidden`, threaded through wire, catalog, and
client; every *presentation* surface (wildcard, name resolution, dup-name
guard, positional aliases, SELECT output, driver rows) operates on visible
columns only. Physical layout, circuits, exchange, checkpoint format: untouched
(**no `STATE_FORMAT` bump**; existing data dirs are invalid because COL_TAB
gains a column — the same situation as when `is_serial` landed, pre-alpha
wipe).

**Visibility rule.** A key slot is *bound* (visible) iff the user's projection
names it; otherwise it is hidden:

- Simple views: a projected source-PK column stays a visible leading column
  under its (possibly aliased) name — `place_pk_front`'s move-to-front stands,
  and "PK columns lead the view schema" remains documented behavior. Its
  auto-*prepend* arm (`project_schema.rs:90-94`) now marks the prepended
  column hidden: `SELECT b FROM t` no longer leaks `a`.
- All six synthetic fabrication sites mark their key columns hidden. Names
  (`_join_pk`, `_set_pk`, …) are kept in COL_TAB as debug labels; they are not
  resolvable.
- GROUP BY natural paths (`group_set_eq_pk` / `is_single_col_natural_pk`,
  `group_by.rs:364-377`) are the bound case and stay visible — the in-place
  PK-slot rename (`group_by.rs:506-546`) is unchanged and becomes the general
  binding mechanism rather than a special case.
- Hidden columns are **not resolvable by name** anywhere (binder, GROUP BY,
  set-op sides, downstream views). Keying downstream work on a view's join or
  group key requires projecting the underlying columns. This deletes a shipped
  capability (`GROUP BY _join_pk`, `test_i128_cross_sign_join.py`) — accepted;
  the key value stays reconstructible from the payload columns that produced
  it (both sides' originals ride the payload on every join shape).

**Wire/catalog representation** (bound and hidden slots both remain physical
columns; `pk_cols` untouched):

- `gnitz-wire/src/flags.rs`: `pub const META_FLAG_HIDDEN: u64 = 4;` (bits 2-7
  are free between `META_FLAG_IS_PK = 2` and the PK-pos byte at bits 8-16).
- `gnitz-core` `ColumnDef` (`types.rs:17-29`): add `is_hidden: bool` plus a
  `fn hidden(mut self) -> Self` chain-builder (mirrors `serial()`).
- Meta-schema codec: `schema_to_batch` sets the bit, `batch_to_schema` reads
  it (`protocol/codec.rs:19-34`, `:82-90`).
- COL_TAB: an 11th column `is_hidden` (u64 0/1) after `is_serial` —
  `append_col_rows` (`client.rs:1391-1410`), `extract_col_entries`
  (`client.rs:1232-1273`), `col_tab_schema`, the engine's COL_TAB definition
  (`catalog/sys_tables.rs:220-239`), and the two engine-side COL_TAB row
  writers that emit one value per column: the sys-table self-description loop
  (`catalog/bootstrap.rs:285`) and `build_col_batch` (`catalog/ddl.rs:711`) —
  each appends a trailing `put_u64(0)`. The engine's
  `scan_column_defs`/`build_schema_from_col_defs` (`catalog/registry.rs`,
  reads indices 4-8 only) ignore the value (the engine has no visibility
  concept); replay validation (`validate_pk_cols`) is unchanged — hidden
  slots are ordinary COL_TAB rows.
- `MAX_COLUMNS` continues to count **physical** columns, hidden included — it
  is the physical-slot/null-bitmap budget, not a UX limit. No relief and no
  change at `create_view_chain` (`client.rs:~896-916`).

**Presentation surfaces switched to visible-only** (add
`Schema::visible_columns()` → `impl Iterator<Item = (usize, &ColumnDef)>` and
`Schema::resolve_visible(name)`; all by-name resolution funnels through it):

- `gnitz-sql` by-name resolution: `bind/resolve.rs::find_unique_column` is the
  chokepoint — route it through `resolve_visible`; likewise the direct-SELECT
  seek/index extractors that match WHERE names against the full schema
  (`dml/plan.rs`: `try_extract_pk_in`, `try_extract_pk_seek_residual`, the
  `collect_index_*` collectors) so `WHERE _join_pk = …` stops resolving.
  `dml/mutate.rs` name resolution is exempt by construction (DML targets are
  base tables; base-table columns are never hidden). `INSERT INTO … SELECT`
  (`dml/insert.rs`) positionally matches the source's projected columns —
  visible-only output is exactly what it must count.
- Every alias-map builder in `plan/view/` (join, exists, set-op, group-by):
  skip hidden.
- Wildcard expansion: `build_projection`'s `SELECT *` arm
  (`project_schema.rs:108-123`), set-op `resolve_set_projection`
  (`set_op.rs:132-188`), and `collect_operator_names`' wildcard fallback over
  a hidden join segment (`dispatch.rs:654-680`) — all expand to visible
  columns only. The set-op change is a **behavior fix**: an upstream synthetic
  PK no longer participates in UNION/INTERSECT/EXCEPT/DISTINCT row identity.
- Duplicate-name guard (`reject_duplicate_column_names`, invoked behind the
  `check_dup_names` flag at `join.rs:840-847`, `exists.rs:713-715`): checks
  visible columns only. The `_src_pk_{i}` fresh-naming stays as a debug label
  but is no longer load-bearing.
- Positional column aliases (`CREATE VIEW v (x, y) AS …`): apply to visible
  columns in order; delete the JOIN-body rejection branch in
  `apply_hidden_column_aliases` (`dispatch.rs:302-331`).
- Query execution: `apply_projection`'s wildcard (`gnitz-sql/src/exec/batch.rs`)
  and `SELECT`-list resolution emit visible columns only.
- Python driver (`gnitz-py/src/lib.rs`): default rows contain visible columns
  only — the filter lands in `build_row_values_into` (`~:966-1007`), the row
  field-name tuple (`~:950`), and `build_field_index` (`~:929-938`), so hidden
  names are neither positional nor attr-resolvable on rows. `scan(...,
  include_hidden=True)` bypasses the filter and surfaces hidden slots at their
  physical positions under their COL_TAB names, decoded from the PK region via
  the existing `PkColumn` machinery. **Schema objects stay physical**:
  `PySchema`/`resolve_table` expose all columns plus their `is_hidden` flags
  (introspection is a debug surface; rows are the presentation surface) — so
  `pk_indices`/`columns[i]`-shaped test assertions keep passing. The
  `include_hidden` scan is the debug surface that preserves the routing↔seek
  e2e tripwire (`test_workers.py::test_view_scan_and_pk_seek` reconstructs the
  pair-PK and seeks it).
- C API: no filtering — it surfaces the physical schema plus the `is_hidden`
  flag; presentation is the binding layers' job.

**Emitter changes** (schema emission only; zero circuit changes):

- `simple.rs`/`project_schema.rs`: `place_pk_front` None-arm prepends hidden.
- `join.rs`: `_join_pk` defs (`:775-784`) and `build_join_view_projection`'s
  `leading_cols` seeding (`:1500-1554`, seed at `:1509`) marked hidden;
  `_pair_pk_{slot}` (`:1104-1124`) hidden.
- `exists.rs`: `_join_pk` (`:481-490`) and `_src_pk_{i}` (`:658-673`) hidden.
- `set_op.rs`: `_set_pk` (`:363-370`) and `_distinct_pk` (`:~408-424`) hidden.
- `group_by.rs`: `_group_pk` (`:378-384`) hidden; both natural arms unchanged.
- `live_columns_projection` (`join.rs:266-320`): its dead-payload pruning is
  cost/width-driven and **stays**; only its "drop the accumulator's leading
  synthetic `_join_pk`" concern becomes automatic (wildcards no longer
  re-admit hidden columns), so the provenance-offset bookkeeping
  (`plan_join_chain`, `join.rs:520-528`) is unchanged but its PK-skip comments
  simplify.

**Semantics note.** A `UNION ALL` view whose branch-salted keys keep identical
payloads distinct now shows two visually identical rows (weights 1+1) instead
of exposing the distinguishing `_set_pk`. That is correct Z-set/bag output; no
client path consolidates by visible columns.

## CLAUDE.md update

In *GnitzDB Theoretical Foundations* §1, after the "PK uniqueness is not a
general invariant" paragraph, insert exactly:

> **Hidden key slots.** A key slot need not be user-visible: synthetic view
> keys (`_join_pk`, `_set_pk`, `_group_pk`, …) and unprojected passthrough PK
> columns are physical schema columns flagged hidden (`META_FLAG_HIDDEN` /
> COL_TAB `is_hidden`). Hidden columns are excluded from wildcard expansion,
> name resolution, duplicate-name checks, and client rows; the PK region,
> routing, sort, and consolidation are unaffected. Base-table columns are
> never hidden.

## Sequencing

- [ ] Wire + core plumbing: `META_FLAG_HIDDEN`, `ColumnDef::is_hidden`,
      meta-schema codec, COL_TAB column (client write/read + engine
      sys-table definition + registry pass-through), `visible_columns()` /
      `resolve_visible()`. No emitter sets the flag yet — behavior-neutral,
      full `make verify` + `make e2e`.
- [ ] Presentation surfaces: binder/alias maps, wildcard expansions, dup-name
      guard, positional aliases (delete the JOIN rejection), `apply_projection`,
      py driver filter + `include_hidden`, capi flag exposure. Still
      behavior-neutral (no column is hidden yet).
- [ ] Flip the emitters: mark the six synthetic sites + `place_pk_front`
      prepend hidden. Rewrite the affected tests in the same commit:
      `test_i128_cross_sign_join.py` (project the key columns explicitly);
      `test_aggregates.py` `_group_pk`-presence → absence;
      `test_multiway_join.py` — both `names.count("_join_pk") == 1` sites
      (`~:245-264` and `~:799`) → zero; `test_workers.py` — one central
      `include_hidden=True` inside the `_view_pairs` helper (`~:1172`, read
      positionally by ~28 range/band-join tests) plus
      `test_view_scan_and_pk_seek` and
      `test_compound_source_pks_wide_pair_pk` (positional `r[0..]` reads);
      `test_dbsp_ops.py` row-shape assertions (`~:264-275`); row-name/shape
      assertions in `test_joins.py`, `test_compound_pk.py`,
      `test_left_join.py`, `test_right_full_join.py`; planner tests asserting
      `_join_pk`/`_group_pk` in output columns → hidden-aware; `_oracle.py`
      docstring tidy (the `project` allow-list is no longer dodging anything).
      `test_null_join_key.py` needs comment updates only (its assertions are
      already name+weight-based). Schema-introspection assertions
      (`pk_indices`, `columns[0].type_code`) keep passing — schema objects
      stay physical.
- [ ] CLAUDE.md paragraph (above).

## Testing

- New e2e (`test_hidden_key_slots.py`, W=4, weight-verified): `SELECT *` over
  join / range-join / EXISTS / set-op / DISTINCT / synthetic-GROUP BY views
  contains no `_`-prefixed key column; a simple view projecting a subset omits
  the unprojected PK and a downstream view over it cannot reference the hidden
  column by name (clean error); a set-op over two join views dedups on
  projected content only (the fixed identity semantics — assert weights);
  `include_hidden=True` surfaces the key slots with correct decoded values;
  view-over-view cascade where every layer's key is hidden.
- Existing suites are the regression net: `make verify` + full `make e2e`
  (W=4) after each checkbox.
