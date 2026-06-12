# Make the single-PK assumption visible

Implicit "this is single-PK" arithmetic littered the codebase and broke
the day compound PKs went user-visible. The explicit `pk_index_single()`
calls were greppable and got audited; the *implicit* sites — local
`+ 1` arithmetic next to a column count, manual indexing into
`pk_cols[0]`, hard-coded one-element slices — were invisible until they
panicked.

Make both visible going forward.

## Scope

In:

- Add named `pk_count()` / `total_cols()` / `num_payload_cols()` helpers
  on `SchemaDescriptor` and `gnitz_core::Schema`, eliminating naked
  `pk_indices().len()` reads and arithmetic with bare literals.
- Replace every `+ 1` (and `- 1`) that combines a column count with a
  PK-related expression with `+ schema.pk_count()` (resp.
  `- schema.pk_count()`). This was the literal shape of the bug in
  `Batch::set_schema` where `payload + 1 == total` silently encoded
  "exactly one PK column."
- Annotate each remaining `pk_index_single()` call site with a comment
  naming the single-PK contract it relies on:
  - System tables / IPC control schema → `// single-PK contract: system schema`
  - Hot paths gated by an explicit `pk_count() == 1` check upstream
    → `// single-PK: caller gated on pk_count==1`
  - Indexed-secondary owner reads → `// TODO(compound-pk-indexes): lift via plans/compound-pk.md`
- Add a `clippy.toml` `disallowed_methods` entry for `pk_index_single`
  with a `reason` pointing to the comment convention above, so new
  call sites land a lint warning that forces the author to either
  justify the single-PK assumption or use `pk_indices()`.

Out:

- Refactoring the legitimate single-PK call sites away from
  `pk_index_single()`. Many catalog / system-table sites genuinely
  *are* single-PK by contract; renaming or restructuring them adds
  churn without value.
- Touching `gnitz-py` / `gnitz-capi` `pk_index_single` reads (covered
  by `plans/compound-pk-python-binding.md` — those need an actual
  binding-surface change, not just an annotation).
- New runtime asserts. The existing `pk_index_single()` debug-assert
  is enough; this plan is about catching the *implicit* sites at
  review time, not at runtime.

## Touchpoints

`crates/gnitz-engine/src/schema.rs`:

- Add `pub fn pk_count(&self) -> usize { self.pk_indices().len() }`
  next to `pk_index_single()`.
- Audit every `+ 1` / `- 1` in `set_schema`, `num_payload_cols`,
  `payload_columns()` users, and shard / batch arithmetic. Replace
  literals with `pk_count()` where the intent is "one slot per PK
  column."

`crates/gnitz-engine/src/storage/shard_reader.rs`:

- `MappedShard::open` computes `num_non_pk = num_cols - 1` and
  `num_regions = 3 + num_non_pk + 1`. The `- 1` silently encodes
  "exactly one PK column"; a zero-payload schema (every column is
  in the PK — the shape of the compound-PK secondary index) reads
  one region short and returns `InvalidShard` on every shard.
  Replace with `schema.num_payload_cols()`. Already fixed once via
  the compound-PK index work; the lint should prevent it from
  reappearing the next time someone refactors `MappedShard::open`.

`crates/gnitz-engine/src/storage/shard_file.rs`:

- `RegionEncoding::Constant { value: [u8; 16] }` and
  `detect_encoding` writing `value[..element_width]` encode
  "PK width never exceeds 16 bytes." For wide compound PKs
  (`pk_stride > 16`) the constant detector overruns the buffer
  and panics in `copy_from_slice`. Audit the buffer width against
  `MAX_PK_BYTES` (or short-circuit Constant detection for
  `element_width > 16`, as the compound-PK fix did). The hazard
  is the implicit "16" literal, not a `pk_index_single()` call —
  the same lint won't catch it, but the same audit pass should.

`crates/gnitz-engine/src/storage/memtable.rs`:

- `MemTable::upsert_sorted_batch` feeds `batch.get_pk(i)` (u128)
  into the bloom filter. `Batch::get_pk` panics in `widen_pk_le`
  for `pk_stride > 16`. The literal hazard is the bloom filter's
  `u128`-keyed interface, not arithmetic. Either skip the bloom
  for wide PKs (the workaround) or move the bloom to a byte-keyed
  interface (the architectural fix — see also
  `plans/wide-pk-trace-cursor.md` for the trace-cursor analogue).

`crates/gnitz-core/src/protocol/types.rs`:

- Same `pk_count()` helper on `gnitz_core::Schema`.

`crates/gnitz-engine/clippy.toml` (new file at workspace root or
crate-level):

```toml
disallowed-methods = [
    { path = "gnitz_engine::schema::SchemaDescriptor::pk_index_single",
      reason = "implicit single-PK assumption — use pk_indices() or annotate the call site with the single-PK contract (see plans/pk-single-assumption-audit.md)" },
]
```

Plus an equivalent entry for `gnitz_core::Schema::pk_index_single` in
`crates/gnitz-core/clippy.toml`.

Existing call sites get `#[allow(clippy::disallowed_methods)]` with the
required comment on the same line; new sites trigger the lint.

## Implementation sketch — comment convention

```rust
#[allow(clippy::disallowed_methods)] // single-PK contract: META_SCHEMA_DESC is always one PK column
let pk_idx = schema.pk_index_single();
```

vs. the new compound-aware form:

```rust
let pk_cols = schema.pk_indices();
```

A PR reviewer scanning a diff for `#[allow(clippy::disallowed_methods)]`
gets a list of every single-PK assumption introduced in that change,
each with its justification on the same line. Today there's no way to
audit that surface.

## Testing

- `cargo clippy -- -D warnings` across the workspace flags any new
  `pk_index_single` call without an inline justification. CI must
  fail on it.
- `cargo test -p gnitz-engine` still passes after the helper / literal
  cleanup — purely a refactor of expression shape, no behavioural
  change.
- A grep audit (one-time, captured in the PR description) over every
  remaining `pk_index_single` call site, classifying it as one of:
  "system schema" / "caller-gated" / "TODO compound-PK-indexes". The
  classification is what reviewers cite when the lint fires in future
  PRs.
- A parallel grep audit (also one-time) over storage-layer `+ 1` /
  `- 1` arithmetic and PK-width literals (`[u8; 16]`,
  `pk_stride.min(16)`, etc.) that aren't routed through
  `pk_count()` / `pk_stride()` / `MAX_PK_BYTES`. The lint covers
  `pk_index_single` but not these implicit-width sites; a one-time
  audit is the only realistic backstop until a property test
  (`plans/storage-data-roundtrip-property-test.md`) makes the
  regression visible at test time. Document the audit in the PR
  description so a future refactor can rerun it as a regression
  check.
