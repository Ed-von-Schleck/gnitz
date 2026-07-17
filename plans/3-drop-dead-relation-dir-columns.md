# Remove the dead `directory` / `cache_directory` catalog columns

The system-catalog tables TABLE_TAB, VIEW_TAB, and IDX_TAB each persist a relation-directory
string column (`directory` on TABLE_TAB, `cache_directory` on VIEW_TAB and IDX_TAB). The engine
never reads any of them for path resolution — every relation directory is **recomputed** from the
live catalog at registration (`hook_table_register` → `table_dir`, `hook_view_register` →
`view_dir`, `index_dir`; `catalog/utils.rs:49-61`) and the in-memory `TableEntry.directory` field
carries the result. No `PAY_DIRECTORY`/`PAY_CACHE_DIRECTORY` payload-index constant exists
(`catalog/sys_tables.rs:166-202`); the columns are written and round-tripped but never consulted.
This removes them.

This is a pre-alpha cleanup with no migration: old data directories carrying the columns are
invalid data. It is **independent of the ALTER work** — no ordering dependency in either
direction — and is the lowest priority of the three plans.

## Why they are dead (verified)

- No engine accessor: the payload-index constants defined at `sys_tables.rs:166-202` cover
  schema_id / name / pk / flags / sql_definition / etc., never `directory`/`cache_directory`.
- Client writes them empty: `create_table` writes `""` (`gnitz-core/src/client.rs:941`),
  `create_view_chain`/`append_view_row` writes `String::new()` (`client.rs:1221`), `create_index`
  and the inline-unique-index path write `""` (`client.rs:616`, `:977`).
- The only non-empty writer is `bootstrap_system_tables` (`catalog/bootstrap.rs:126`), which
  writes a real `{sys_dir}/{name}` into TABLE_TAB `directory` for each system-table
  self-registration row — still never read back.
- The columns **are** decoded client-side, but only to reproduce the exact payload byte-for-byte
  in a `-1` retraction row so it cancels the `+1` under `(PK, payload)` consolidation
  (`TableRecord.directory` decoded at `client.rs:1593`, copied into the drop row at `:1001`;
  `ViewRecord.cache_directory` at `:1611`, copied at `:1626`; the IDX `cache_directory` at
  `drop_index_by_name`, `:637`, copied at `:648`). Once the columns are gone, those decode reads
  and retraction copies simply disappear — the `-1` row is built from the remaining columns.

## Edit surface

**Wire schema (`gnitz-wire/src/catalog.rs`):** remove `col("directory", …)` from `TABLE_TAB_COLS`
(`:71`), `col("cache_directory", …)` from `VIEW_TAB_COLS` (`:85`), and `col("cache_directory", …)`
from `IDX_TAB_COLS` (`:121`). Each removal drops one payload column and shifts the payload indices
of every column defined **after** it in that table.

**Payload-index constants (`catalog/sys_tables.rs:166-202`):** do **not** hand-edit these. Every
`TABTAB_PAY_*` / `VIEWTAB_PAY_*` / `IDXTAB_PAY_*` constant is computed by name via
`pay_index_in` / `col_index_in` (`gnitz-wire/src/catalog.rs:33`, a `const fn` name lookup), not a
literal — removing a `col(...)` entry re-derives them at compile time, and a stale *name* fails to
compile rather than silently misnumbering. The only manual step is a grep to confirm no code uses a
**literal** payload index in place of a constant for these three tables (the client hard-coded
decode indices below are the ones to re-derive). `IDX_TAB`'s `cache_directory` is the **last**
column (index 6), so nothing follows it — zero renumbering there.

**Engine writers:** `bootstrap.rs:126` (drop the computed-path field from the TABLE_TAB seed row);
`catalog/ddl.rs:16` `idx_tab_row` (production IDX_TAB writer used by FK auto-indexes — drop the
`""` field); `catalog/ddl.rs:226` (test-only `create_table` shortcut — drop the `table_dir(...)`
field). Any engine code that reads a payload by index after the removed column must use the
renumbered constant.

**Client (`gnitz-core/src/client.rs`):** remove the `directory` field from `TableRecord` (`:133`)
and the `cache_directory` field from `ViewRecord` (`:144`); drop the corresponding decode reads
(`:1593` col 3 for TABLE_TAB, `:1611` col 4 for VIEW_TAB, `:637` col 6 for IDX_TAB — these
hard-coded column indices shift and must be re-derived from the renumbered layout); drop the
retraction-copy of those fields (`:1001`, `:1626`, `:648`, `:1284`); drop the empty-string writes
at create sites (`:616`, `:941`, `:977`, `:1221`).

## Tests

- The existing catalog/DDL round-trip and drop tests must pass with the narrower sys-table
  schemas: CREATE then DROP of a table, a view, and a unique index each still cancels to empty
  under consolidation (the `-1` retraction row is built from the remaining columns and byte-equals
  the `+1`).
- A fresh server boots, self-registers its system tables, and answers a catalog scan with the new
  TABLE_TAB/VIEW_TAB/IDX_TAB column sets (bootstrap-seed regression for the removed
  `directory` field).
- FK auto-index creation (`idx_tab_row`) round-trips through the narrower IDX_TAB.
