# LIKE / ILIKE pattern matching

## 1. Problem

`WHERE name LIKE 'foo%'` is unsupported: `Expr::Like`/`Expr::ILike` fall into
`bind_structural`'s catch-all (`crates/gnitz-sql/src/bind/structural.rs:216-218`). With the
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
  **character** per the fn2 lenient UTF-8 rule (a char starts at every byte with
  `(b & 0xC0) != 0x80`), so `_` never splits a multibyte code point on valid UTF-8 and
  stays total on arbitrary bytes.
- **Escape**: default `\` (PostgreSQL/MySQL-compatible); `ESCAPE 'c'` overrides with a
  single ASCII character (multi-char or non-ASCII → Bind error); `ESCAPE ''` disables
  escaping entirely. Inside the pattern, the escape character makes the following character
  literal (`\%` = literal `%`, `\\` = literal `\`, `\a` = literal `a` — MySQL-style, total).
  A pattern **ending** in a live escape character is a Bind error ("LIKE pattern must not
  end with escape character", the PostgreSQL message). All of this is validated at bind —
  the literal-only rule means no runtime pattern error path exists.
- **ILIKE folds case ASCII-only** (the fn2 rule 4): pattern literals are ASCII-lowercased
  once at compile; haystack bytes compare via `eq_ignore_ascii_case`. `ILIKE 'straße%'`
  matches `STRAßE…` (the ASCII letters fold; ß is byte-identical) but not `STRAẞE…` or
  `STRASSE…` (capital ẞ and the SS expansion are different bytes — no Unicode folding).
  Documented deviation.
- **NULL operand → NULL** (row excluded in predicate position). The pattern is never NULL
  (literal). `NULL LIKE 'x'` → NULL (the operand takes the fn2 LitNull-in-string-context
  rule: `load_null_str`).
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

## 3. Engine (`crates/gnitz-engine/src/expr/`)

### 3.1 Matcher compilation (resolve-time, `program.rs`)

`LogicalInstr::StrLike { dst, src, pat_idx, ci: bool }` (one variant, `ci` from the wire
opcode — the `StrOp` collapse pattern) → `Instr::StrLike { dst, src, matcher_idx, ci }`.
`resolve` parses each referenced pool entry once into a `LikeMatcher` pushed onto
`ResolvedProgram.like_matchers: Vec<LikeMatcher>` (the `int_sets` pattern,
`program.rs:786-807`):

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
`StrColConst` arm's check, `program.rs:952-963`) and requires a non-empty entry (≥ the
header byte); `src` must be a `Str`-class register, `dst` gets `Scalar` — the fn2 in-order
class walk covers both — and `StrLike` joins the fn2 `dst ≠ operand` anti-aliasing arm
(the same deliberate belt-and-suspenders as `StrCmp`).

### 3.2 Eval arm (`batch.rs`)

`Instr::StrLike` is a string-consuming bool producer, shaped exactly like `StrCmp`: per row
`regs[dst] = matcher.matches(view_bytes(..), ci) as i64`, then the full `StrCmp` null
epilogue — `null_copy1` (operand NULL → NULL) **+ `zero_null_rows`** (uniform with fn2's
string compares; consumers mask by null bits anyway, but the register value stays clean) —
and `maybe_pack_bool_bits`. Matching:

- `Exact`: length + memcmp (CI: `eq_ignore_ascii_case`).
- `Prefix`/`Suffix`: length check + memcmp of the affix window.
- `Contains`: first-byte skip scan (position on the needle's first byte — both CI variants
  of it for ILIKE — then window compare); no external deps, O(n·m) worst case on
  adversarial needles, linear in practice.
- `Generic`: the standard iterative glob: consume tokens left to right; on `AnyMany`,
  record a backtrack point (pattern index, haystack index) and on mismatch resume there
  with the haystack advanced one **character** (the lenient char step); `AnyOne` advances
  one character; `Lit` compares bytes (CI-aware). Terminates in O(n·m); no recursion.

Analyses: `each_reg_read` reads `src`; `classify_registers` marks `dst` a bool producer,
`src` neither mask (a `Str` lane); `is_strictly_non_nullable`: propagates only → safe
remainder (join `StrCmp`).

## 4. Client side

### 4.1 Binding (`crates/gnitz-sql/src/bind/structural.rs`)

New arms for `Expr::Like { negated, any, expr, pattern, escape_char }` and `Expr::ILike
{ … }` (sqlparser 0.62 `ast/mod.rs:988-1014`):

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

`expr_operands` (`crates/gnitz-sql/src/ast_util.rs:135-176`) **and its rebuild twin
`map_structural_operands`** (`ast_util.rs:185-238`, whose doc commits "a node recursed by
one rewriter is recursed by all") gain `Expr::Like`/`Expr::ILike` arms contributing `expr`
**and** `pattern` — the fn1 walker invariant (an `EXCLUDED` reference or nested aggregate
under either sub-expression must stay visible to `expr_contains_excluded`/
`expr_has_aggregate`/`count_side_refs`, and the scalar-subquery rewriter must recurse the
operand rather than clone the node through).

### 4.2 IR + lowering (`ir.rs`, `lower.rs`)

`BoundExpr::Like { expr, pattern, escape, ci }`; `infer_type` → I64 (boolean).

`OpcodeBackend::like`: lower `expr` through the string channel (must produce `Str`, with
the fn2 LitNull interception; a numeric operand → typed error
`"LIKE requires a string operand"`); encode the pool entry
`[escape_byte_or_0, pattern_bytes…]` via `add_const_bytes`; emit `str_like`/`str_ilike`
(two `ExprBuilder` emitters, `binary_op` shape with `pat_idx` in `a2`). Patterns are
byte-transparent in the pool — the tokenizer applies escapes engine-side, so client and
engine agree on exactly one encoding.

`InterpBackend::like` → `Unsupported` (strings are engine-only, fn2 rule 6).

## 5. Edit surface

| File | Change |
|---|---|
| `crates/gnitz-wire/src/expr.rs` | 2 opcode constants (76, 77) |
| `crates/gnitz-core/src/expr.rs` | `str_like`/`str_ilike` emitters |
| `crates/gnitz-sql/src/ir.rs` | `BoundExpr::Like` variant; `infer_type` arm |
| `crates/gnitz-sql/src/bind/structural.rs` | `Expr::Like`/`Expr::ILike` arms; escape/pattern validation |
| `crates/gnitz-sql/src/ast_util.rs` | `expr_operands` + `map_structural_operands` arms for `Expr::Like`/`Expr::ILike` |
| `crates/gnitz-sql/src/lower.rs` | `like` backend method + dispatch |
| `crates/gnitz-sql/src/exec/eval.rs` | `like` → `Unsupported` |
| `crates/gnitz-engine/src/expr/program.rs` | `StrLike` variants; `LikeMatcher` compile at resolve; validate + analyses arms |
| `crates/gnitz-engine/src/expr/batch.rs` | `StrLike` eval arm + matcher `matches` |

## 6. Tests

**Engine unit:** each matcher shape × LIKE/ILIKE over a batch with NULL rows, inline and
long (heap) strings: exact/prefix/suffix/contains hits and misses; `_` across multibyte
UTF-8 (`'_ö_'`), `%` matching empty, `'%'` matches `''`, `''` matches only `''`; escaped
`\%`/`\_`/`\\`; generic backtracking cases (`'%a_c%'` against overlapping candidates,
adversarial `'%aa%aa%'` repetition); **specialization-shape pins**: `'a_c%'` compiles to
Generic (never `Prefix("a_c")`), `'%'` to `Prefix("")`; ILIKE ASCII folding incl.
non-ASCII pass-through (the ß/ẞ matrix from §1); forged-blob totality (trailing escape
byte, empty pool entry rejected, `pat_idx` OOB, non-`Str` operand class, dst-operand
aliasing).

**SQL unit:** bind matrix — non-literal pattern, `LIKE ANY`, bad escapes (two chars,
non-ASCII, trailing live escape) rejected; `ESCAPE ''` disables (`'100\%'` then matches a
literal backslash); `NOT LIKE`/`NOT ILIKE` wrap in Not; `UPPER(s) LIKE 'A%'` lowers through
the string channel; `NULL LIKE 'x'` compiles to a NULL result; `EXCLUDED.x` under a LIKE
pattern position is caught by the insert guard (walker).

**E2E (`crates/gnitz-py/tests/test_like.py`, `GNITZ_WORKERS=4`):** views filtered by each
pattern shape with insert/update/delete incrementality; `ILIKE` case matrix; `NOT LIKE`;
LIKE over a computed operand (`TRIM(s) LIKE …`); ad-hoc SELECT with LIKE; a point-DML
`DELETE … WHERE s LIKE …` errors with the residual interpreter's typed `Unsupported` —
the same behavior string equality predicates have in DML today (`mutate.rs` routes WHERE
through `bind_residuals`/`eval_expr`, which rejects string operands; the error surfaces
per-row *before* any write is built), pinned for consistency. The test must insert a
matching-candidate row first — an empty candidate set never evaluates the predicate and
the DELETE succeeds with count 0.

## 7. Sequencing

- [ ] Commit 1: wire + emitters + engine (matcher compile, eval arm, validation, analyses)
      + engine tests.
- [ ] Commit 2: binder + IR + lowering + interp rejection + SQL unit tests + E2E suite.
