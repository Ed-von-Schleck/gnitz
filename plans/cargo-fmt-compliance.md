# Make the workspace `cargo fmt`-clean

## Goal

`make verify` cannot currently pass: its `fmt-check` step
(`cd crates && cargo fmt --all --check`) fails on `HEAD` because the source is
hand-aligned and was never run through stable rustfmt. Bring the whole workspace
to `cargo fmt` compliance at `max_width = 120` so `cargo fmt --all --check` is
clean and stays clean — **without** one unreviewable ~196-file diff and
**without** wrecking `git blame`.

End state:
- `crates/rustfmt.toml` sets `max_width = 120`.
- Every crate has been run through `cargo fmt`, one commit per crate (7 commits).
- `.git-blame-ignore-revs` lists those 7 commits; `git blame` skips them.
- `cd crates && cargo fmt --all --check` exits 0; `make verify` passes.

## Current state (validated 2026-06-18)

- rustfmt `1.9.0-stable`. No `rustfmt.toml`, no `rust-toolchain` pin, no
  `.git-blame-ignore-revs`; `blame.ignoreRevsFile` unset.
- Workspace manifest is `crates/Cargo.toml` (`[workspace]`); the Makefile runs
  fmt via `cd crates && cargo fmt`. Member edition `2021`.
- House style hand-aligns colons in struct literals, `const` declarations, and
  field lists (e.g. `pub const WAL_HEADER_SIZE:   usize = 48;`,
  `tid:         batch.pks.get(i)`); stable rustfmt collapses these to single
  spaces. This is the bulk of the churn.
- A full `cargo fmt --all` rewrites ~196 files (+37,279 / −15,288 lines). At
  `max_width = 120` only 272 source lines exceed the width (985 exceed 100), and
  most long lines are doc-comments, which stable rustfmt does not rewrap.
- Per-crate `.rs` counts: `gnitz-capi` 1, `gnitz-py` 1, `gnitz-test-harness` 1,
  `gnitz-sql` 8, `gnitz-wire` 12, `gnitz-core` 16, `gnitz-engine` 151.

## Design

### 1. Add `crates/rustfmt.toml` first (its own commit)

```toml
# crates/rustfmt.toml
max_width = 120
```

rustfmt resolves config from each file's directory upward, so placing it beside
the workspace manifest covers every crate. Edition is supplied by `cargo fmt`
from each manifest (2021), so it is not set here. Commit this alone — it changes
no code but fixes what "fmt-clean" means for every later step.

Commit: `style: add rustfmt.toml (max_width 120)`.

### 2. One `cargo fmt` commit per crate, smallest first

Order — ascending size, to shake out the mechanics on tiny crates before the
151-file engine (fmt is per-file mechanical, so dependency order is irrelevant):

`gnitz-capi` → `gnitz-py` → `gnitz-test-harness` → `gnitz-sql` → `gnitz-wire` →
`gnitz-core` → `gnitz-engine`.

For each crate `C`:

```bash
cd crates
cargo fmt -p C                       # formats lib + bins + tests targets of C
cargo build  -p C
cargo clippy -p C --all-targets      # warnings are errors; fmt must add none
cargo test   -p C                    # behavior-preserving; confirm green
git add -A && git commit -m "style(C): format with rustfmt (max_width 120)"
```

`cargo fmt -p C` formats every target of the package, including the `tests/`
integration files, so each crate is fully covered by its single commit.

### 3. Preserve genuinely tabular blocks with `#[rustfmt::skip]`

Some constructs are intentionally aligned as *data*, not prose — lookup/dispatch
tables, the region-layout constants, aligned test matrices — where de-alignment
hurts readability. Where a crate's fmt diff mangles such a block, add
`#[rustfmt::skip]` to that item and re-run `cargo fmt -p C` (skipped blocks stay
`--check`-clean), keeping it inside the same per-crate commit so the count stays
at seven. Restrict skips to genuine data tables; prose and ordinary code take the
rustfmt result. Most crates need none.

### 4. Record the fmt commits in `.git-blame-ignore-revs` (final commit)

After the 7 fmt commits land, create `.git-blame-ignore-revs` at the repo root
listing their full 40-char hashes (one per line; `#` comments allowed):

```
# Bulk rustfmt migration — formatting only, not semantic changes.
<hash of style(gnitz-capi)>
<hash of style(gnitz-py)>
<hash of style(gnitz-test-harness)>
<hash of style(gnitz-sql)>
<hash of style(gnitz-wire)>
<hash of style(gnitz-core)>
<hash of style(gnitz-engine)>
```

Activate it for local blame:

```bash
git config blame.ignoreRevsFile .git-blame-ignore-revs
```

GitHub honors `.git-blame-ignore-revs` automatically; the `git config` line makes
local `git blame` skip them too. List only the seven per-crate fmt commits and
the rustfmt.toml/this commit are ordinary.

Commit: `chore: add .git-blame-ignore-revs for the rustfmt migration`.

### 5. Gate

No Makefile change is needed: `fmt-check` already runs `cargo fmt --all --check`,
which goes green once the last crate (`gnitz-engine`) is formatted. Between the
first and last fmt commit, `make verify` / `fmt-check` is expected to fail for the
not-yet-formatted crates — use per-crate `cargo fmt -p C --check` during the
transition; `make verify` is meaningful again only after the final crate commit.

## Verification

- Per crate: `cargo build -p C`, `cargo clippy -p C --all-targets`,
  `cargo test -p C` stay green. Clippy reads the AST/HIR, so formatting cannot
  change its result; build/test confirm no parse error or mis-placed skip.
- A fmt commit is trustworthy as mechanical iff re-running `cargo fmt -p C` on its
  parent reproduces it exactly (modulo the few `#[rustfmt::skip]` markers).
- Final: `cd crates && cargo fmt --all --check` exits 0; `make verify` passes;
  `git blame` on a reformatted line still attributes to the pre-migration author.

## Scope / non-goals

- Behavior-preserving only: no logic edit rides along in a fmt commit; the sole
  manual additions are `#[rustfmt::skip]` markers on tabular blocks.
- `max_width = 120` and otherwise rustfmt defaults; no other style knobs pinned.
- No `rust-toolchain` pin and no CI are added (the repo has neither today);
  contributors must run a rustfmt whose output matches `1.9.0-stable`.
