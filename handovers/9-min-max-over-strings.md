# 9. `MIN` / `MAX` over STRING (and UUID / U128 / BLOB)

**Status:** research handover, not a plan. Self-contained — no other file is needed.

## Provenance of the evidence below

- **Tree:** `main` at `fb013479`, with an uncommitted in-flight refactor across
  `crates/gnitz-engine/src/query/compiler/*` and `crates/gnitz-engine/src/catalog/write_path.rs`.
  Code is cited by **file and symbol only, never by line number** — symbols were verified to
  exist at the time of writing; line numbers would already be stale (they moved under that
  refactor during this session).
- **How the SQL results were obtained:** temporary integration tests in throwaway
  `git worktree`s (since removed; the working tree was never modified), calling
  `SqlPlanner::execute` on one statement at a time and printing accept/reject plus the error
  text. Server: `GNITZ_SERVER_BIN=<repo-root>/gnitz-server`, the prebuilt binary at the repo
  root dated 2026-08-31 13:35 — **not verified to have been built from `fb013479`**. The Rust
  test harness has no staleness check (that check lives in the Python E2E suite).
- **Workers: 1.** `ServerHandle::start()` is `start_n(1)`. Nothing here was exercised at W>1,
  so no claim below covers exchange, fanout or partitioning behaviour.
- **"OK" below means the planner and the engine's circuit compiler accepted the statement. It
  does NOT mean the resulting view produces correct rows.** No probe inserted a row, read a
  result, or compared weights against an oracle. Before building on any statement shown as
  compiling, validate its semantics — in a Z-set engine that means checking weights, not row
  presence. Any cost or complexity claim not backed by a quoted measurement is analytic
  reasoning from the code, and is labelled as such where it appears.

## Fixtures the quoted statements ran against

```sql
CREATE TABLE t  (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL,
                 b BIGINT NOT NULL, f DOUBLE NOT NULL, s TEXT NOT NULL, sn2 TEXT);
CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, ts BIGINT NOT NULL, user_id BIGINT NOT NULL,
                 amt BIGINT NOT NULL, s TEXT NOT NULL);
```

Not every column appears in every quoted statement. `sn2` is the only nullable TEXT column.


Assessed as poor value-per-effort **despite** being genuinely unreachable with no workaround,
because reading the implementation showed the cost is much higher than the surface suggests.
This note exists mainly to stop a fresh session from mistaking it for a small fix.

## Current state

```
CREATE VIEW w5  AS SELECT k, MIN(s) AS ms FROM t GROUP BY k  -> unsupported: MIN: not supported on String columns
SELECT k, MIN(s) AS ms FROM t GROUP BY k                     -> same (ad-hoc path, same message)
CREATE VIEW w5b AS SELECT k, MAX(s) AS ms FROM t GROUP BY k  -> unsupported: MAX: not supported on String columns
```

Gate: `reject_min_max_unorderable` (`gnitz-sql/src/agg.rs`), which fires when
`!has_scalar_register(ty)`; `has_scalar_register` is `ScalarKind::from_type_code(tc).is_some()`
(`gnitz-sql/src/types.rs`). Its doc comment states the reason: MIN/MAX have no correct
accumulator path for wide (U128/UUID/I128) types since the i64 slot cannot hold them, Blob
has no ordering, and integer widening would read a String's prefix as LE signed i64, which
orders by neither bytes nor signedness.

No workaround was found. Verified adjacent facts:
- `COUNT(s)` on a string column works (`CREATE VIEW t5d AS SELECT user_id, COUNT(s) …` → OK).
- `GROUP BY <string column>` works (`CREATE VIEW g23 AS SELECT s, COUNT(*) … GROUP BY s` → OK),
  because the group key **hashes** the string's content rather than ordering it:
  `hash_group_col` → `hash_german_string_content` (`gnitz-store/src/schema/key.rs`) feeds
  a 4-byte LE length prefix plus the content into the group-fold digest, which becomes the
  synthetic U128 `_group_pk` of `ReduceOutKey::SyntheticFold`. **Grouping is not ordering** —
  a digest has no order, so this mechanism does not carry over to MIN/MAX.
- `MIN` over a derived integer works (`t5b`: `LENGTH(s) AS l`, then `t5c`: `MIN(l)` → OK),
  which gives you the min *length*, not the min string.

## Why it is expensive — from reading the code

`crates/gnitz-store/src/ops/reduce/avi.rs`, the combined aggregate-value index:

- Key layout is `(group key ‖ ordinal: U8 ‖ value: U64)`, built by pushing every column with
  `push_pk` — **the index is a PK-only table**, so every component must be fixed-width and
  OPK-encodable.
- `av_encode` produces a `u64` order-preserving image, bit-inverted for a MAX ordinal so the
  index's ascending walk yields that ordinal's extreme first.
- One compile-time assertion pins the layout: `const _: () = assert!(ORDINAL_BYTES == 1 &&
  VALUE_BYTES == 8);`

`crates/gnitz-store/src/ops/reduce/agg.rs`:

- `Accumulator` holds `acc: i64` and, for MIN/MAX, that i64 *is* a MIN-oriented `u64` order
  image (`seed_encoded_extreme`, `fold_extreme`, `get_value_bits` → `kind.order_inverse`).
- `const _: () = assert!(std::mem::size_of::<Accumulator>() <= 24);` — with the comment that
  `AdhocFold` holds `groups × aggregates` of these.

A string has no fixed-width order-preserving **injective** image. Supporting it therefore
appears to require, at minimum: a prefix key plus a payload tiebreak, an AVI schema that is
no longer PK-only, a probe walk that scans the tied-prefix run instead of taking the first
entry, and a redesign of `Accumulator` away from a single i64 under a 24-byte budget.

**The pathological case:** a column whose values share long prefixes (URLs, paths, prefixed
IDs) degrades the tie-run scan toward a full-group scan on every retraction. Whatever design
is chosen has to answer for that.

## Explicitly NOT verified

- **The prefix+tiebreak sketch above is my design speculation, not a finding.** It was not
  compared against alternatives and may not be the right approach. Do not treat it as a
  starting design.
- The AVI **probe walk** in `op_reduce` was not read in full — only `av_encode` and the
  schema construction. The actual retraction path may impose constraints not captured here.
- `ADHOC_GROUP_CAP` was referenced in comments but its value was not read, so the ad-hoc
  fold's memory budget for a string-carrying accumulator is unquantified.
- Whether UUID and U128 (fixed-width, 16 bytes, genuinely ordered) are a *separate, much
  easier* item than String was not investigated. They plausibly are — the blocker for them
  is the 8-byte value column, not the absence of an order — and splitting them out may be
  the cheap win hiding in this note.
