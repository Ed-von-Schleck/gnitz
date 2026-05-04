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
were in `compiler.rs` alone. Missing any one compiles cleanly and mishandles UUID columns
silently at runtime.

## Goal

Replace raw `type_code::*` usage throughout the engine with `TypeCode::*` enum arms from
`gnitz-core`. Every exhaustive match in the engine becomes a compile-time enforced check:
adding a new variant causes a cascade of compiler errors at every site that needs
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
match TypeCode::from_validated_u8(col.type_code) {
    TypeCode::U64 => ...
    TypeCode::U128 => ...
    // No _ arm — exhaustiveness is enforced by the compiler.
    // If multiple variants share a path, list them explicitly:
    //   TypeCode::U8 | TypeCode::I8 => ...
}
```

Add a `TypeCode::from_validated_u8` helper to gnitz-core for use at conversion sites:

```rust
impl TypeCode {
    /// Converts a wire u8 to TypeCode. Panics on unknown codes.
    /// Only valid for type codes that have already passed DDL validation.
    #[inline]
    pub fn from_validated_u8(v: u8) -> Self {
        // TypeCode is #[repr(u8)] with discriminants == wire constants, so transmute is
        // sound for any value that passed DDL validation. The try_from path is kept for
        // the panic message.
        Self::try_from_u64(v as u64)
            .unwrap_or_else(|_| panic!("invalid type_code {} in validated schema", v))
    }
}
```

`TypeCode` is already `#[repr(u8)]` with discriminants equal to the wire constants, so
this conversion is safe for any value that has passed DDL validation. This function must
only be called on engine-internal data that has already passed DDL validation. Any
network-facing or pre-validation DDL code parsing raw bytes must use the underlying safe
`TryFrom` path to avoid panic on corrupted input.

## Critical rule: no wildcard arms on TypeCode matches

**Never write `_ =>` in a match on a TypeCode value.** Wildcards are the root cause of
silent type mishandling. If multiple variants share a code path, enumerate them explicitly
with `|`:

```rust
TypeCode::U64 | TypeCode::U32 | TypeCode::I64 | TypeCode::I32 | ... => { ... }
```

If a site genuinely cannot handle a variant (e.g., `promote_col_to_u64` cannot handle
U128), use an explicit `TypeCode::U128 | TypeCode::UUID => panic!(...)` arm with a
clear message.

## Performance: hoist conversions and structure lookups outside row loops

`from_validated_u8` must never be called inside a per-row loop. Column type codes are
constant per column descriptor. Resolve them once before the loop:

```rust
// Correct: convert once per column, outside the loop
let tc = TypeCode::from_validated_u8(col.type_code);
for row in 0..batch.count {
    match tc { ... }
}
```

This applies equally to per-column constants like `cs` (column stride) and `pi` (payload
index). In `promote_col_to_u64`, `cs` is computed from `col_type_code` inside the
function, which is itself called per row. The caller should compute `cs` once and pass it
in, or the function should be restructured so type dispatch happens before the loop.

### Sort comparator hoisting in `reduce.rs`

`compare_by_group_cols` is the inner comparison function passed to `sort_unstable_by`
during `argsort_delta`. It is evaluated O(N log N) times and currently looks up
`schema.columns[c_idx].type_code` and computes `payload_idx(c_idx, pki)` dynamically on
every call.

Before calling `sort_unstable_by`, map `group_by_cols` into a small `Vec` of sort
descriptors containing pre-computed `payload_idx`, `col_size`, and `TypeCode`. The
closure captures this descriptor array instead of the schema pointer, eliminating schema
indirection and type-code validation from the hot comparison path.

This is the most significant performance opportunity in this refactor because
`compare_by_group_cols` is called O(N log N) times per `op_reduce` invocation.

## Files to modify

### Phase 1: Cargo dependency

**`crates/gnitz-engine/Cargo.toml`**
```toml
[dependencies]
gnitz-wire = { path = "../gnitz-wire" }
gnitz-core = { path = "../gnitz-core" }   # add this line
```

Add `use gnitz_core::protocol::types::TypeCode;` (confirm the import path with
`cargo doc -p gnitz-core`) to `gnitz-engine/src/schema.rs` or a new `prelude` module.

### Phase 2: Match-arm conversion (by file, largest first)

**`src/ops/reduce.rs`** — the largest single engine file by line count. Contains many
`match col.type_code` arms for aggregation operator construction. Convert each to
`TypeCode::from_validated_u8(col.type_code)`.

`AggDescriptor` is an in-memory-only `#[repr(C)]` struct (not serialized to disk). Change
`col_type_code: u8` to `col_type_code: TypeCode` directly. Since `TypeCode` is
`#[repr(u8)]`, the size/alignment static assertions will continue to hold (verify). This
eliminates the conversion at every use inside `AggDescriptor` methods.

Apply sort comparator hoisting (see above) when converting `compare_by_group_cols` and
`argsort_delta`.

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

**`src/catalog/utils.rs`** — `get_index_key_type()` match. With TypeCode the wildcard
`_ => Err("Unknown column type")` arm is eliminated entirely: replace it with an
exhaustive match where `TypeCode::F32 | TypeCode::F64 | TypeCode::String => Err(...)`.
This forces whoever adds `TypeCode::Timestamp` to explicitly decide if that type is
indexable rather than silently getting a runtime error.

**`src/catalog/store.rs`** — `build_schema_from_col_defs` iterates `col_defs` and writes
`SchemaColumn { type_code: cd.type_code, ... }`.

Note: the engine has its own `ColumnDef` in `catalog/types.rs` with `type_code: u8`.
This is distinct from `gnitz-core::protocol::types::ColumnDef` which has
`type_code: TypeCode`. `build_schema_from_col_defs` uses the engine's internal struct.
The write `type_code: cd.type_code` is `u8 → u8` today; it stays as-is (no conversion
needed at the write site). Matches on `cd.type_code` elsewhere in the same file do need
conversion.

**`src/ops/linear.rs`**, **`src/ops/index.rs`** — schema construction in linear map and
index lookup operators.

In `index.rs`, `promote_col_to_u64` computes `cs` with a manual match:
```rust
let cs = match col_type_code {
    type_code::U8 | type_code::I8 => 1,
    type_code::U16 | type_code::I16 => 2,
    type_code::U32 | type_code::I32 | type_code::F32 => 4,
    _ => 8,
};
```
This is a duplicate of `crate::schema::type_size()`. Delete the match and replace with
`let cs = crate::schema::type_size(col_type_code) as usize;`. Then restructure the
callsite so `cs` is computed before the loop (it is column-constant) rather than
recomputed on every call.

In `linear.rs`, `promote_col_to_pk` currently has a `_ =>` arm that reads 8 bytes for
all remaining types regardless of their actual stride. With TypeCode this becomes
exhaustive: UUID gets the 16-byte u128 path; small integers (U8/U16/U32/I8/I16/I32) use
`type_size()` to read the correct number of bytes.

**`src/storage/` files** (columnar.rs, memtable.rs, table.rs, shard_index.rs,
partitioned_table.rs) — these mostly construct temporary `SchemaDescriptor` objects from
existing column arrays. The `type_code` field is copied from existing `SchemaColumn`
objects, not matched on. Most of these need no match conversion — only sites that
dispatch behavior based on type code need the enum.

**`src/runtime/master.rs`** — `format_pk_value` formats a PK u128 for error messages.
After conversion:

```rust
fn format_pk_value(pk: u128, schema: &SchemaDescriptor) -> String {
    let pk_col = schema.columns[schema.pk_index as usize];
    match TypeCode::from_validated_u8(pk_col.type_code) {
        TypeCode::U128 => format!("{}", pk),
        TypeCode::UUID => format_uuid_hyphenated(pk),
        TypeCode::I64  => format!("{}", pk as u64 as i64),
        TypeCode::I32  => format!("{}", pk as u64 as i32),
        TypeCode::I16  => format!("{}", pk as u64 as i16),
        TypeCode::I8   => format!("{}", pk as u64 as i8),
        TypeCode::U64 | TypeCode::U32 | TypeCode::U16 | TypeCode::U8 => format!("{}", pk as u64),
        TypeCode::F32 | TypeCode::F64 | TypeCode::String =>
            panic!("format_pk_value: non-integer PK type"),
    }
}
```

No `_` arm. The UUID case uses a local `format_uuid_hyphenated(v: u128) -> String`
helper with the same byte layout as gnitz-py's `format_uuid`:
```rust
fn format_uuid_hyphenated(v: u128) -> String {
    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (v >> 96) as u32,
        (v >> 80) as u16,
        (v >> 64) as u16,
        (v >> 48) as u16,
        v & 0x0000_ffff_ffff_ffff)
}
```

### Phase 3: Remove the re-export alias (optional cleanup)

`src/schema.rs` currently does `pub use gnitz_wire::type_code;` to expose the raw
constants. Once all internal match arms use `TypeCode::*`, this re-export is only needed
for external callers that still use the raw constants (e.g., test helpers). Evaluate
whether to remove it or keep it for backward compatibility with test code.

## Approach: incremental conversion

Do NOT attempt to convert all 326 sites in one PR. The risk of a missed or incorrect
conversion is high and the diff will be unreviewable. Instead:

1. Add the Cargo dependency and confirm `cargo build -p gnitz-engine` succeeds.
2. Add `TypeCode::from_validated_u8` to gnitz-core.
3. Convert one file at a time, running `cargo test -p gnitz-engine` after each file.
4. For each file, identify every match on a type code. Convert to `TypeCode` and enumerate
   all variants explicitly. No `_` arms.
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
- `grep -n "_ =>" crates/gnitz-engine/src/` has no hits in TypeCode match contexts.
- Adding a new `TypeCode` variant to gnitz-wire and gnitz-core produces compiler errors
  in gnitz-engine that point exactly to every match site requiring an update.

## Estimated scope

- Cargo.toml change: 1 line.
- `from_validated_u8` helper: ~5 lines in gnitz-core.
- Match arm conversions: ~326 raw usages; roughly 30-40 are in match arms. The rest are
  comparisons (`col.type_code == type_code::U64`) or struct initializers
  (`SchemaColumn { type_code: type_code::U64, ... }`) which need different handling.
- `AggDescriptor` field change: ~5 lines plus verification that layout assertions hold.
- `promote_col_to_u64` dead code removal: delete the `cs` match, replace with
  `type_size()`, restructure callsite loop.
- Sort comparator hoisting in `reduce.rs`: pre-compute `SortDescriptor` array before
  `sort_unstable_by`, replace schema lookups in `compare_by_group_cols` closure.
- Realistically 4-6 hours of focused mechanical work plus test cycles.

## What this does NOT fix

- The Python `PyTypeCode` classattrs remain a manual mirror of the wire constants. A
  follow-up change should either generate them from the wire constants at build time or
  add a build-time assertion (e.g., a `build.rs` script that reads gnitz-wire and emits
  a test that the Python constants match). That is a separate smaller task.
- `SchemaColumn.type_code` remains `u8`. This is correct — changing it would be an
  on-disk format break.
