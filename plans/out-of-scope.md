## What is explicitly out of scope

- **`INSERT INTO t SELECT …`** — use a view.
- **Auto-increment PK** — SEQ_TAB exists but is not yet wired to SQL.
- **Ad-hoc non-indexed SELECT** — use `CREATE VIEW` or `CREATE INDEX`.
- **Multi-column indices** — only single-column supported by `IndexCircuit`.
- **Float/string column indices** — `get_index_key_type()` explicitly rejects them (Phase B).
- **Range queries on string indices** — hashing destroys order; equality only.
- **`UPDATE … RETURNING`** — gnitz has no server-side row returning.
- **Transactions / rollback** — gnitz is Z-set append-only; no rollback.
- **Foreign key constraints** — gnitz does not enforce them via SQL.
- **`ORDER BY` in views** — not incrementalisable; views are unordered Z-sets.
  `ORDER BY` in a top-level SELECT is client-side sort (future addition to SELECT).
- **Window functions** — possible future addition, not in this plan.
- **Multi-table UPDATE / DELETE** — use a view to compute the target set, then push.
- **`INSERT OR IGNORE`** — the upsert (INSERT OR REPLACE) semantics are inherent;
  ignore-on-duplicate would require a seek-before-insert which conflicts with the
  bulk-insert model.
- **`INTERSECT ALL` / `EXCEPT ALL` on multisets** — for `unique_pk=true` (default),
  ALL variants are equivalent to non-ALL. Multiset weight-aware variants are deferred.
- **`WITH RECURSIVE`** — requires fixpoint operators not present in gnitz DBSP.
- **Correlated subqueries / scalar subqueries** — `SELECT (SELECT ...)` and correlated
  `WHERE` subqueries require per-row evaluation; emit `GnitzSqlError::Unsupported`.
- **Full NULL-aware `NOT IN`** — `NOT IN (subquery)` uses NULL-filter + anti-join
  (correct when subquery has no NULLs). Strict SQL semantics (return empty when any
  NULL in subquery) deferred. Use `NOT EXISTS` for NULL-safe semantics.
