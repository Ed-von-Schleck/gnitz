# LIKE / ILIKE pattern matching

## 1. Problem

`WHERE name LIKE 'foo%'` is unsupported: `Expr::Like`/`Expr::ILike` fall into
`bind_structural`'s catch-all (`crates/gnitz-sql/src/bind/structural.rs:190-192`). With the
string register class in place (string values flow through `Str`-class registers; operand
loads via `LOAD_COL_STR`, computed strings via the string channel), LIKE is one
string-consuming boolean opcode away.

This plan adds `LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE`, with the `ESCAPE` clause, over any
string expression operand — `WHERE UPPER(name) LIKE 'A%'` works. Explicitly excluded, as a
decision: regex (`~`, `REGEXP`, `SIMILAR TO`) — a regex engine is a dependency with
pathological-backtracking runtime hazards and no place in the committed surface; and
`LIKE ANY (…)` (Snowflake's `any: true` parse) — rejected at bind.

### Committed semantics

- **Pattern and escape must be string literals.** A non-literal pattern (`a LIKE b`) is a
  Bind error: per-row pattern compilation is exactly the performance pitfall this design
  exists to exclude (the matcher is compiled once per program at resolve, never per row).
- `%` matches any sequence of zero or more characters; `_` matches exactly one character —
  **character** meaning the engine's lenient UTF-8 rule: a character starts at every byte
  with `(b & 0xC0) != 0x80`, so `_` never splits a multibyte code point on valid UTF-8 and
  stays total on arbitrary bytes (STRING is byte-transparent in the engine; UTF-8 is
  validated only at the client decode boundary).
- **Escape**: default `\` (PostgreSQL/MySQL-compatible); `ESCAPE 'c'` overrides with a
  single ASCII character (multi-char or non-ASCII → Bind error); `ESCAPE ''` disables
  escaping entirely. Inside the pattern, the escape character makes the following character
  literal (`\%` = literal `%`, `\\` = literal `\`, `\a` = literal `a` — MySQL-style, total).
  A pattern **ending** in a live escape character is a Bind error ("LIKE pattern must not
  end with escape character", the PostgreSQL message). All of this is validated at bind —
  the literal-only rule means no runtime pattern error path exists.
- **ILIKE folds case ASCII-only** (the engine has no Unicode tables and byte-transparent
  values make locale folding a hazard): pattern literals are ASCII-lowercased once at
  compile; haystack bytes compare via `eq_ignore_ascii_case`. `ILIKE 'straße%'` matches
  `STRAßE…` (the ASCII letters fold; ß is byte-identical) but not `STRAẞE…` or `STRASSE…`
  (capital ẞ and the SS expansion are different bytes — no Unicode folding). Documented
  deviation.
- **NULL operand → NULL** (row excluded in predicate position). The pattern is never NULL
  (literal). `NULL LIKE 'x'` → NULL (the operand takes the LitNull-in-string-context rule:
  `load_null_str`).
- Deterministic, total, stateless — a linear map operator like every fn1/fn2 function.

## 2. Wire (`crates/gnitz-wire/src/expr.rs`)

Two opcodes, continuing after fn2's 75:

```rust
/// SQL LIKE over a string register: `[op, dst, str_reg, pat_idx]` → 0/1.
/// `pat_idx` is a const-pool index; the entry is `[escape_byte, pattern…]`
/// (escape_byte 0 = escaping disabled). The matcher is compiled at resolve.
pub const EXPR_STR_LIKE: u32 = 76;
/// ASCII-case-insensitive LIKE, same operands and pool format.
pub const EXPR_STR_ILIKE: u32 = 77;
```

NOT LIKE is `bool_not` over the result (the opcode-40-45 NE pattern); no negated opcodes.

## 3. Engine (`crates/gnitz-expr/src/`)

### 3.1 Matcher compilation (resolve-time, `program.rs`)

`LogicalInstr::StrLike { dst, src, pat_idx, ci: bool }` (one variant, `ci` from the wire
opcode — the `StrOp` collapse pattern) → `Instr::StrLike { dst, src, matcher_idx, ci }`.
`resolve_program` parses each referenced pool entry once into a `LikeMatcher` pushed onto
`ResolvedProgram.like_matchers: Vec<LikeMatcher>` (the `int_sets` pattern,
`program.rs:830-858`, field at `:1204-1208`):

```rust
/// A compiled LIKE pattern. Specialized shapes cover the overwhelmingly common
/// patterns with one memcmp-class check per row; Generic is the classic
/// greedy-with-backtracking glob walk (worst-case O(n·m), n = value bytes,
/// m = pattern tokens — no regex, no unbounded state).
enum LikeMatcher {
    Exact(Vec<u8>),            // no wildcards        s LIKE 'abc'
    Prefix(Vec<u8>),           // lit% (one trailing %)
    Suffix(Vec<u8>),           // %lit
    Contains(Vec<u8>),         // %lit%
    Generic(Vec<LikeTok>),     // everything else
}
enum LikeTok { Lit(Vec<u8>), AnyOne, AnyMany }  // literal run / `_` / `%`
```

Tokenization applies the escape byte (header byte 0 ⇒ none), merges adjacent literals,
collapses `%%` runs, then specializes **on the exact token sequence** — `[Lit]` → Exact,
`[Lit, AnyMany]` → Prefix, `[AnyMany, Lit]` → Suffix, `[AnyMany, Lit, AnyMany]` → Contains,
`[AnyMany]` (a bare `'%'`) → `Prefix(vec![])` (matches everything, incl. `''`), anything
else → Generic. The rule is token-shape, never string-shape: `'a_c%'` tokenizes as
`[Lit, AnyOne, Lit, AnyMany]` and is Generic, NOT `Prefix("a_c")`. For ILIKE the `Lit`
bytes are ASCII-lowercased here (after tokenization, so an alphabetic escape char is never
corrupted).

A forged blob's pattern is total by construction: a trailing live escape byte tokenizes as
a literal escape byte (the client binder never emits one — Bind error — but the engine
stays panic-free on any bytes). `validate` bounds `pat_idx` against the const pool (the
`StrColConst` arm's `ConstIdxOutOfRange` check, `program.rs:1029-1042`) and rejects an empty
entry — it carries no escape header byte — with a new `ExprValidateErr::LikePatternEmpty
{ const_idx }`, the same shape as `IntSetNotAligned` for a malformed pool entry. `src` must
be a `Str`-class register and `dst` gets `Scalar` — the in-order class walk covers both —
and `StrLike` joins the `dst ≠ operand` anti-aliasing arm (the same deliberate
belt-and-suspenders as `StrCmp`).

### 3.2 Eval arm (`batch.rs`)

`Instr::StrLike` is a string-consuming bool producer, shaped exactly like `StrCmp`
(`batch.rs:425-442`): per row `regs[dst] = matcher.matches(view_bytes(..), ci) as i64`, then
the full `StrCmp` null epilogue — `null_copy1` (operand NULL → NULL, `:191-204`) **+
`zero_null_rows`** (`:360-367`; uniform with the string compares — consumers mask by null
bits anyway, but the register value stays clean) — and `maybe_pack_bool_bits` (`:322-330`).
Matching:

- `Exact`: length + memcmp (CI: `eq_ignore_ascii_case`).
- `Prefix`/`Suffix`: length check + memcmp of the affix window.
- `Contains`: first-byte skip scan (position on the needle's first byte — both CI variants
  of it for ILIKE — then window compare); no external deps, O(n·m) worst case on
  adversarial needles, linear in practice.
- `Generic`: the standard iterative glob: consume tokens left to right; on `AnyMany`,
  record a backtrack point (pattern index, haystack index) and on mismatch resume there
  with the haystack advanced one **character** (the lenient char step); `AnyOne` advances
  one character; `Lit` compares bytes (CI-aware). Terminates in O(n·m); no recursion.

Analyses: `classify_registers` (`program.rs:1337-1425`) marks `dst` a bool producer and
reads `src` as a value (`read!(non_bool_read, src)` — a `Str` lane is in neither bool mask);
`is_strictly_non_nullable` (`:1431-1487`): propagates only → safe remainder (join `StrCmp`).

## 4. Client side

### 4.1 Binding (`crates/gnitz-sql/src/bind/structural.rs`)

New arms in `bind_structural`'s match (`:67`) for
`Expr::Like { negated, any, expr, pattern, escape_char }` and `Expr::ILike { … }`
(sqlparser 0.62 `ast/mod.rs:988-1014`; `escape_char: Option<ValueWithSpan>`):

- `any: true` → `Unsupported("LIKE ANY is not supported")`.
- `pattern` must bind to `BoundExpr::LitStr`, else
  `Unsupported("LIKE pattern must be a string literal")` — including `s LIKE NULL`, which
  PostgreSQL evaluates to NULL; here it is a Bind error (a consequence of the literal-only
  rule, documented deviation).
- `escape_char`: absent → `Some(b'\\')`; a one-ASCII-char literal → that byte; `''` →
  `None`; anything else → Bind error.
- Escape validation per §1 (trailing-live-escape → Bind error).
- Result: `BoundExpr::Like { expr, pattern: String, escape: Option<u8>, ci: bool }`,
  wrapped in `UnaryOp(Not, …)` when `negated`.

`expr_operands` (`crates/gnitz-sql/src/ast_util.rs:270-312`) gains `Expr::Like`/`Expr::ILike`
arms contributing `expr` **and** `pattern` — the walker invariant its doc states, and what
keeps an `EXCLUDED` reference, a nested aggregate, or a subquery under either sub-expression
visible to `expr_contains_excluded` (`dml/insert.rs:376-381`), `expr_has_aggregate` /
`for_each_agg_call` (`ast_util.rs:188`, `:220`) and the subquery detectors (`:344`, `:356`).

### 4.2 IR + lowering (`ir.rs`, `lower.rs`)

`BExpr::Like { expr, pattern, escape, ci }`; `infer_type_with` (`ir.rs:95-149`) → I64
(boolean); the four `BExpr` walkers (`try_map_refs`, `try_expand_leaves`, `for_each_ref`,
`OpcodeBackend::lower`) each gain their arm.

`OpcodeBackend::like` (a private method beside `binop`/`case`/`in_list`): lower `expr`
through the string channel (must produce `ExprKind::Str`, with the LitNull interception; a
numeric operand → typed error `"LIKE requires a string operand"`); encode the pool entry
`[escape_byte_or_0, pattern_bytes…]` via `add_const_bytes` (`gnitz-core/src/expr.rs:258-262`);
emit `str_like`/`str_ilike` (two `ExprBuilder` emitters, `binary_op` shape with `pat_idx` in
`a2`). Patterns are byte-transparent in the pool — the tokenizer applies escapes
engine-side, so client and engine agree on exactly one encoding.

## 5. Edit surface

| File | Change |
|---|---|
| `crates/gnitz-wire/src/expr.rs` | 2 opcode constants (76, 77) |
| `crates/gnitz-core/src/expr.rs` | `str_like`/`str_ilike` emitters |
| `crates/gnitz-sql/src/ir.rs` | `BExpr::Like` variant; `infer_type_with` + walker arms |
| `crates/gnitz-sql/src/bind/structural.rs` | `Expr::Like`/`Expr::ILike` arms; escape/pattern validation |
| `crates/gnitz-sql/src/ast_util.rs:270-312` | `expr_operands` arms for `Expr::Like`/`Expr::ILike` |
| `crates/gnitz-sql/src/lower.rs` | `like` method + `lower` dispatch arm |
| `crates/gnitz-expr/src/program.rs` | `StrLike` variants; `LikeMatcher` compile at resolve; validate (+`LikePatternEmpty`) + analyses arms |
| `crates/gnitz-expr/src/batch.rs` | `StrLike` eval arm + matcher `matches` |

## 6. Tests

**Engine unit (`crates/gnitz-expr/src/{program,batch}/tests.rs`):** each matcher shape ×
LIKE/ILIKE over a batch with NULL rows, inline and long (heap) strings: exact/prefix/suffix/
contains hits and misses; `_` across multibyte UTF-8 (`'_ö_'`), `%` matching empty, `'%'`
matches `''`, `''` matches only `''`; escaped `\%`/`\_`/`\\`; generic backtracking cases
(`'%a_c%'` against overlapping candidates, adversarial `'%aa%aa%'` repetition);
**specialization-shape pins**: `'a_c%'` compiles to Generic (never `Prefix("a_c")`), `'%'`
to `Prefix("")`; ILIKE ASCII folding incl. non-ASCII pass-through (the ß/ẞ matrix from §1);
forged-blob totality (trailing escape byte, empty pool entry rejected, `pat_idx` OOB,
non-`Str` operand class, dst-operand aliasing).

**SQL unit:** bind matrix — non-literal pattern, `LIKE ANY`, bad escapes (two chars,
non-ASCII, trailing live escape) rejected; `ESCAPE ''` disables (`'100\%'` then matches a
literal backslash); `NOT LIKE`/`NOT ILIKE` wrap in Not; `UPPER(s) LIKE 'A%'` lowers through
the string channel; `NULL LIKE 'x'` compiles to a NULL result; `EXCLUDED.x` under a LIKE
pattern position is caught by the insert guard (walker).

**E2E (`crates/gnitz-py/tests/test_like.py`, `GNITZ_WORKERS=4`):** views filtered by each
pattern shape with insert/update/delete incrementality; `ILIKE` case matrix; `NOT LIKE`;
LIKE over a computed operand (`TRIM(s) LIKE …`); ad-hoc SELECT with LIKE; and point-DML
`DELETE … WHERE s LIKE …` / `UPDATE … WHERE s ILIKE …`, which work because the DML residual
runs the same compiled program through the same evaluator (`exec/residual.rs`) — pinned so
the shared-evaluator property does not silently regress into a per-surface rejection.

## 7. Sequencing

- [ ] Commit 1: wire + emitters + engine (matcher compile, eval arm, validation, analyses)
      + engine tests.
- [ ] Commit 2: binder + IR + lowering + SQL unit tests + E2E suite.
