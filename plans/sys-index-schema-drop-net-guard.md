# INDEX / SCHEMA drop: per-PK net-weight guard (catalog positivity)

The `sys_indices` and `sys_schemas` families are the only rewrite-pair-incapable catalog
families whose `precheck_family` arm applies **no per-PK net-weight guard**, so a retraction
of an already-dead row lands a **persistent net-negative ghost** in the sys store — a violation
of the base-table positivity invariant (a durable sys row with net weight `< 0`, which
consolidation never drops because it only drops net-**zero**). This is latent today (masked, no
functional impact) and pre-existing; it is spun out of the DROP COLUMN plan because it is
independent of column ALTERs.

## The gap

`precheck_family` (`catalog/write_path.rs:414`) routes `SysFamily::Schema | SysFamily::Index` to
the empty-contract arm (`write_path.rs:434`, `=> Vec::new()`), which skips
`precheck_retraction_contract` (`write_path.rs:251-346`) entirely. That contract is the **only**
site computing the per-PK `net = live_weight + Σ batch weights` and rejecting `net ∉ {0,1}`
(`write_path.rs:316-322`). Table/View/Column run it; Index/Schema do not.

Downstream, the INDEX drop arm (`write_path.rs:583-608`) even **`continue`s past a `-1` whose
index row is already gone** (`:597-599`, `if !cursor.valid || key != idx_id { continue; }`) —
returning `Ok` and letting `apply_local` write the orphan `-1`. The SCHEMA drop arm
(`write_path.rs:569-577`) checks only member-count, never net weight.

### Concrete reproduction (no ALTER needed)

Two connections each hold a stale snapshot of a live index `i` (owner table, one covering
column) and both issue `DROP INDEX i`:

1. Conn A: `DROP INDEX i` → precheck passes (i live), `apply_local` retracts i (net → 0), fsync,
   commit. `i` is now net-dead.
2. Conn B: `DROP INDEX i` (its client `-1` built while i was live) applies under the serialized
   catalog write lock: the FK-target guard `continue`s on the now-missing row (`:597-599`),
   returns `Ok`; `apply_local` writes the raw `-1` → **`sys_indices` net weight for `i` = −1**, a
   persistent ghost. No panic (`hook_index_register`'s drop branch no-ops on the missing
   circuit), no wrong result (index reads and `replay_system_table(Index)` gate on **positive**
   weight, and index ids are never recycled), but the catalog now holds a durable negative row.

The identical shape reproduces for `sys_schemas` via two racing `DROP SCHEMA` of an
already-empty, already-dropped schema.

## The fix

Give the INDEX and SCHEMA arms the same per-PK net-weight guard the relation/column families
already have. The **net∈{0,1} check is the load-bearing part** (it needs no byte-exact `-1`
payload): for a retraction of an already-dead row, `net = 0 + (−1) = −1`, rejected as "catalog
changed concurrently"; a legitimate drop of a live row gives `net = 0`, accepted.

Add, in `precheck_family` **before** the existing Index/Schema drop guards (i.e. compute it for
the negative-weight PKs the same way `precheck_retraction_contract` does at `write_path.rs:261-322`,
but only the CAS-free net portion):

```
// For every distinct PK carrying a weight<0 row in this INDEX/SCHEMA batch:
//   net = seek_live_sys_row(family, pk).weight + Σ_batch weights for pk
//   reject if net ∉ {0,1}  ("catalog changed concurrently: retracting a
//   system-catalog row that no longer exists / would go negative").
```

`seek_live_sys_row` (`write_path.rs:292`, the same primitive the contract uses) reads the net
live weight through the merged cursor. Place the guard:

- **INDEX** (`write_path.rs:583`): after the `ctx.in_cascade_drop()` early-return (`:584-586`)
  and before the FK-index / FK-target guards. A DROP TABLE cascade is byte-exact and nets each
  index to 0 (so it would pass), but it is already skipped by the `in_cascade_drop` return, so
  the net guard only ever runs for a **standalone** `DROP INDEX` — exactly the racing case. The
  `:597-599` `continue`-on-missing then becomes unreachable for a live drop and dead code for a
  dead one (the net guard rejects first); simplify it to an `expect` once the guard lands.
- **SCHEMA** (`write_path.rs:569`): before the member-count guard (`:571-575`). A `SCHEMA_TAB -1`
  is never submitted from inside an engine cascade (`write_path.rs:566-568`), so no cascade
  exemption is needed.

**Optional payload CAS.** If `GnitzClient::drop_index` / `drop_schema` reproduce the live row's
full payload byte-exactly at `-1` (as `drop_table` does — verify against `client.rs`), the arm
can route through `precheck_retraction_contract(family, batch, <NAME_PAY>, /*is_relation*/ false)`
instead, which layers the payload CAS on top of the net guard (a better error on a torn/stale
snapshot). The pair-field name-only check inside the contract never fires for these families
(they carry no `(-1,+1)` rewrite pair). If the client `-1` is **not** byte-exact, use the
net-only guard above — do **not** route through the full contract, or legitimate drops reject.

## Tests

Rust (engine, `catalog` tests):
- Racing standalone `DROP INDEX i` (i already net-dead): the second submission is rejected
  "catalog changed concurrently", `sys_indices` net weight for `i` stays `0` (no negative ghost);
  a single legitimate `DROP INDEX` still succeeds; a DROP TABLE cascade still retracts the owner's
  indices (the `in_cascade_drop` skip is preserved).
- Racing `DROP SCHEMA` of an already-dropped empty schema: second rejected, no negative
  `sys_schemas` row; a legitimate `DROP SCHEMA` on an empty schema still succeeds; the
  member-not-empty guard still fires first for a populated schema.
- `make verify` green.
