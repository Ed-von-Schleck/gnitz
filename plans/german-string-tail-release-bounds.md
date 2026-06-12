# Release-mode bounds handling in `german_string_tail`

## What

`german_string_tail` (`crates/gnitz-engine/src/schema.rs`, ~line 515) reads a
long German-string suffix from a blob using a stored heap offset, guarded only
by a `debug_assert!`:

```rust
let heap_offset = read_u64_le(s, 8) as usize;
debug_assert!(
    heap_offset + end <= blob.len(),
    "german_string_tail: long string [{}..{}) overruns blob (len={})",
    heap_offset + 4, heap_offset + end, blob.len(),
);
&blob[heap_offset + 4..heap_offset + end]
```

In release builds the `debug_assert!` is compiled out, so a corrupt
`heap_offset` reaches the slice and triggers a slice-index panic.

## Severity (read before scoping)

This is **not** the "crashes the whole node" hazard it first appears to be, for
two reasons:

1. **Reads run under `guard_panic`.** Scan/read handlers wrap their work in
   `catch_unwind` and convert any panic into a clean `Err(...)` (the worker
   process survives). A slice-index OOB here becomes a returned error, not a
   process abort.
2. **The blob is trusted on-disk data**, written by the engine's own
   `relocate_german_string_vec`. A bad offset means internal storage
   corruption / an engine bug, not attacker-controlled wire input.

So this is a low-severity hardening item, not a security or availability fix.

## What NOT to do

Do **not** return an empty slice (`&[]`) on a failed bounds check. The function
returns the string's bytes; substituting `&[]` for a corrupt long string is
**silent data corruption** — the caller would observe an empty string instead of
either the correct value or a loud failure. Silent wrong answers are worse than
a contained panic for a database.

## Change

Keep the loud-failure behavior but make it explicit and release-active, so the
failure is a precise, located error rather than an opaque slice-index panic that
only `catch_unwind` happens to contain:

- Promote the check to a release-active `assert!` (or an explicit
  `panic!` with the same precise message) so the panic carries the
  `heap_offset`/`end`/`blob.len()` context in every build. Combined with panic
  source-location reporting in `guard_panic`, this turns an opaque
  "internal server error" into an actionable, located message.
- Alternatively, if a non-panicking contract is wanted, change the signature to
  return `Result<&[u8], _>` and thread the error up to the read handler. This is
  a larger ripple (the function returns a borrowed slice today) and is only
  worth it if other corrupt-data read paths are converted to typed errors at the
  same time.

Prefer the first option: it is a two-line change, preserves the existing
panic→`Err` containment, and never invents data.

## Testing

- Unit test: construct a blob with a deliberately out-of-range `heap_offset` and
  assert the call fails (panics with the precise message, caught as `Err` by the
  guarded handler) rather than returning a truncated/empty slice.
- `make test` must stay green; existing valid German-string round-trip tests are
  unaffected.
