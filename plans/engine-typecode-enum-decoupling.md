# Engine TypeCode Enum Decoupling

## Problem

`gnitz-engine` depends on `gnitz-wire` but not on `gnitz-core`. Throughout the engine,
type codes are used as raw `u8` values imported via `gnitz_wire::type_code::*`. There are
326 such usages in engine source (excluding tests), covering all 13 current type codes.

When a new `TypeCode` variant is added, the engine must be manually audited and patched at
every match site. There is no compile-time signal for a missed site. A raw `u8` match arm
silently falls through to the `_` case if its constant is not listed; the compiler does
not treat this as an exhaustiveness error because matching on `u8` is not exhaustive in
the enum sense.

The UUID implementation required nine separate engine sites to be updated. Four of them
were in `compiler.rs` alone (`agg_value_idx_eligible`, `make_agg_output_schema_single_gc`,
GROUP BY hash-key exclusion, join-schema column encoding). Missing any one would have
compiled cleanly and mishandled UUID columns silently at runtime.

## Goal

Replace raw `type_code::*` usage throughout the engine with `TypeCode::*` enum arms from
`gnitz-core`. This makes every exhaustive match in the engine a compile-time enforced
check: adding a new variant causes a cascade of compiler errors at every site that needs
updating, exactly as happens in `gnitz-core` and `gnitz-sql` today.

## Dependency graph verification

Current graph (no circular dependencies):

```
gnitz-wire  ←  gnitz-core  ←  gnitz-sql  ←  gnitz-py
gnitz-wire  ←  gnitz-engine
```

Adding `gnitz-engine → gnitz-core`:

```
gnitz-wire  ←  gnitz-core  ←  gnitz-sql  ←  gnitz-py
gnitz-wire  ←  gnitz-core  ←  gnitz-engine
```

`gnitz-core` depends on `gnitz-wire` and `libc` only. It has no dependency on
`gnitz-engine`, so no cycle is introduced. Verify with `cargo metadata` before starting.

## What does NOT change

- `SchemaColumn.type_code` stays as `u8` — it is part of the wire-format struct that is
  memcpy'd into shard files and WAL blocks. Changing its field type would break on-disk
  compatibility.
- `SchemaDescriptor` layout is unchanged.
- `gnitz-wire::type_code` constants stay — they are still used by gnitz-core (as the
  enum discriminant values) and by any FFI or wire-level code.
- The engine's internal `schema.rs` re-export of `gnitz_wire::type_code` can remain as a
  convenience alias as long as it is not used in match arms.

## Key challenge: u8 field vs TypeCode enum at use sites

The central tension: `SchemaColumn.type_code` is `u8`, but we want to match on
`TypeCode`. At every use site, the conversion is:

```rust
// Before
match col.type_code {
    type_code::U64 => ...
    type_code::U128 => ...
    _ => ...
}

// After
match TypeCode::try_from_u64(col.type_code as u64) {
    Ok(TypeCode::U64) => ...
    Ok(TypeCode::U128) => ...
    Ok(tc) => panic!("unhandled type code {:?}", tc),   // exhaustiveness via enum
    Err(_) => panic!("unknown type code {}", col.type_code),
}
```

The `Ok(tc) => panic!` arm is the key: it is NOT a wildcard `_` — if a new TypeCode
variant is added, this arm no longer covers all `Ok(...)` cases and the compiler errors.
However, this requires that every use site that was previously a silent wildcard becomes
an explicit panic, which is a judgment call for each site.

Alternative: add a `TypeCode::from_u8` helper that returns `TypeCode` directly (panicking
on unknown codes) for use in internal hot paths that have already validated the schema at
DDL time:

```rust
impl TypeCode {
    /// Converts a wire u8 to TypeCode. Panics on unknown codes.
    /// Only valid for type codes that have already passed DDL validation.
    pub fn from_validated_u8(v: u8) -> Self {
        Self::try_from_u64(v as u64)
            .unwrap_or_else(|_| panic!("invalid type_code {} in validated schema", v))
    }
}
```

This reduces verbosity at call sites where the schema has already been validated.

## Files to modify

### Phase 1: Cargo dependency

**`crates/gnitz-engine/Cargo.toml`**
```toml
[dependencies]
gnitz-wire = { path = "../gnitz-wire" }
gnitz-core = { path = "../gnitz-core" }   # add this line
```

Add `use gnitz_core::protocol::types::TypeCode;` (or the public re-export path) to
`gnitz-engine/src/schema.rs` or a new `prelude` module. Confirm the import path with
`cargo doc -p gnitz-core`.

### Phase 2: Match-arm conversion (by file, largest first)

**`src/ops/reduce.rs`** — the largest single engine file by line count. Contains many
`match col.type_code` arms for aggregation operator construction. Convert each to
`TypeCode::from_validated_u8(col.type_code)`. The `match_` on the returned enum is then
exhaustive.

**`src/compiler.rs`** — four sites already known from the UUID implementation:
- `agg_value_idx_eligible` (~line 866): exclusion check for UUID/U128
- `make_agg_output_schema_single_gc` (~line 808): GROUP BY output shortcut
- GROUP BY hash-key eligibility (~line 1492)
- Join schema column encoding (~line 707): the loop that builds output column types

Each of these currently compares against raw `u8` constants. Converting to enum match
arms will make the exhaustiveness visible.

**`src/catalog/hooks.rs`** — PK type validation and `hook_index_register`. Currently
uses `type_code::U64 | type_code::U128 | type_code::UUID` pattern with raw constants.

**`src/catalog/ddl.rs`** — PK type guard, view column output type handling.

**`src/catalog/utils.rs`** — `get_index_key_type()` match.

**`src/catalog/store.rs`** — `build_schema_from_col_defs` iterates `col_defs` and writes
`SchemaColumn { type_code: cd.type_code, ... }`. The `type_code` field in `ColDef` is
already a `TypeCode` enum from gnitz-core (set via `sql_type_to_typecode`). The write to
`SchemaColumn.type_code` (a `u8`) becomes `cd.type_code as u8`. No match needed here;
just confirm the cast is correct.

**`src/ops/linear.rs`**, **`src/ops/index.rs`** — schema construction in linear map and
index lookup operators.

**`src/storage/` files** (columnar.rs, memtable.rs, table.rs, shard_index.rs,
partitioned_table.rs) — these mostly construct temporary `SchemaDescriptor` objects from
existing column arrays. The `type_code` field is copied from existing `SchemaColumn`
objects, not matched on. Most of these need no match conversion — only sites that
dispatch behavior based on type code need the enum.

**`src/runtime/master.rs`** — schema column decoding from wire.

### Phase 3: Remove the re-export alias (optional cleanup)

`src/schema.rs` currently does `pub use gnitz_wire::type_code;` to expose the raw
constants. Once all internal match arms use `TypeCode::*`, this re-export is only needed
for external callers that still use the raw constants (e.g., test helpers). Evaluate
whether to remove it or keep it for backward compatibility with test code.

## Approach: incremental conversion

Do NOT attempt to convert all 326 sites in one PR. The risk of a missed or incorrect
conversion is high and the diff will be unreviable. Instead:

1. Add the Cargo dependency and confirm `cargo build -p gnitz-engine` succeeds.
2. Add `TypeCode::from_validated_u8` to gnitz-core.
3. Convert one file at a time, running `cargo test -p gnitz-engine` after each file.
4. For each file, identify match arms that should be exhaustive (all type codes handled
   explicitly) vs. arms that legitimately use `_` (e.g., "all numeric types get a
   fixed-width stride; string gets special handling"). Exhaustive matches become enum
   matches; legitimate wildcard matches can keep `_` but should document what the
   wildcard means.
5. Run `make e2e` after each major subsystem (reduce, compiler, catalog).

## Success criteria

- `cargo build -p gnitz-engine` produces zero warnings (warnings are errors in this
  crate).
- `make test` passes (626+ unit tests).
- `make e2e` passes (665+ E2E tests).
- `grep -rn "type_code::[A-Z]" crates/gnitz-engine/src/` returns zero results in match
  arm positions (i.e., no raw constant used as a match pattern). The module import
  (`use gnitz_wire::type_code`) and constant references in non-match contexts (e.g.,
  `SchemaColumn { type_code: type_code::U64, ... }` initializers) may remain if
  appropriate.
- Adding a new `TypeCode` variant to gnitz-wire and gnitz-core produces compiler errors
  in gnitz-engine that point exactly to every match site requiring an update.

## Estimated scope

- Cargo.toml change: 1 line.
- `from_validated_u8` helper: ~5 lines in gnitz-core.
- Match arm conversions: ~326 raw usages; roughly 30-40 are in match arms. The rest are
  comparisons (`col.type_code == type_code::U64`) or struct initializers
  (`SchemaColumn { type_code: type_code::U64, ... }`) which need different handling
  (comparisons become `TypeCode::from_validated_u8(col.type_code) == TypeCode::U64` or
  the inverse).
- Realistically 4-6 hours of focused mechanical work plus test cycles.

## What this does NOT fix

- The Python `PyTypeCode` classattrs remain a manual mirror of the wire constants. A
  follow-up change should either generate them from the wire constants at build time or
  add a build-time assertion (e.g., a `build.rs` script that reads gnitz-wire and emits
  a test that the Python constants match). That is a separate smaller task.
- `SchemaColumn.type_code` remains `u8`. This is correct — changing it would be an
  on-disk format break.
