# Limitations

What gnitz refuses today that no other page covers, and what to do instead.
Each reference page states what it refuses beside what it accepts. The planner
refuses each of these with an error naming it; nothing is silently ignored.

```sql
CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT, region TEXT);
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer BIGINT, amount BIGINT);
```

## Subqueries

**A subquery inside an `EXISTS` or `IN` subquery.** Put the inner one into a
view of its own and read that.

**A subquery in a query with `GROUP BY` or aggregates.** Put the subquery's
part into an inner view and aggregate over it:

```sql,refused
CREATE VIEW regional AS
    SELECT region, COUNT(*) AS n FROM customers
    WHERE id IN (SELECT customer FROM orders) GROUP BY region;
```

```text
WHERE: a subquery is not supported together with GROUP BY/aggregates; put the subquery in an inner view
```

```sql
CREATE VIEW buying_customers AS
    SELECT id, region FROM customers WHERE id IN (SELECT customer FROM orders);
CREATE VIEW regional AS SELECT region, COUNT(*) AS n FROM buying_customers GROUP BY region;
```

**A scalar subquery other than one aggregate.** `(SELECT SUM(…) FROM … WHERE
correlated)` works; a subquery returning a plain column does not. Write it as
a join.

**`NOT IN` over nullable columns.** SQL's `NOT IN` gives no rows at all once
the subquery holds a `NULL`, so gnitz requires both sides to be `NOT NULL`. Use
`NOT EXISTS` with an equality:

```sql,refused
CREATE VIEW idle AS SELECT id FROM customers WHERE id NOT IN (SELECT customer FROM orders);
```

```text
NOT IN (SELECT …) requires the outer operand and the subquery column to be NOT NULL (SQL's NULL semantics diverge from the anti-join otherwise); use NOT EXISTS with an explicit equality instead
```

```sql
CREATE VIEW idle AS
    SELECT id FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer = c.id);
```

**The same relation in a query and its subquery** needs an alias on at least
one of them; the planner does not let the inner name shadow the outer one.

**`LATERAL` and `WITH RECURSIVE`** are not supported.

## Aggregates and windows

- **`FILTER (WHERE …)`** on an aggregate: use `SUM(CASE WHEN … THEN x END)`.
- **`DISTINCT` aggregates over different arguments** in one query, such as
  `COUNT(DISTINCT a)` beside `COUNT(DISTINCT b)`: compute each in a view of
  its own.

```sql,refused
CREATE VIEW reach AS
    SELECT region, COUNT(DISTINCT name) AS names, COUNT(DISTINCT id) AS ids
    FROM customers GROUP BY region;
```

```text
DISTINCT aggregates: every DISTINCT aggregate of one query must read the same argument
```

- **`STRING_AGG`, `ARRAY_AGG`, `BOOL_AND`/`BOOL_OR`, percentiles, `STDDEV`**
  are not available.
- **`LAG`, `LEAD`, `NTILE`, `FIRST_VALUE`, `LAST_VALUE`, `PERCENT_RANK`,
  `CUME_DIST`** are not available as window functions.

## Expressions

- **No regular expressions** (`SIMILAR TO`, `REGEXP_*`) and **no bitwise
  operators**.
- **No non-deterministic functions** (`RANDOM()`, `CURRENT_DATE`, …): a view must give
  the same rows each time it is computed. Generate random values in the client.
- **128-bit columns** (`UUID`, `HUGEINT`, `UHUGEINT`) can be stored, selected,
  grouped by, joined on and used as keys, but not compared with a value or used
  in arithmetic. A plain `SELECT` of one row by a `UUID` primary key works.

## Writes

**`TRUNCATE`** is not supported: `DELETE FROM t`.

## Operations

gnitz is a single server: there is no replication or failover. Access control
is all-or-nothing: an authenticated client may run any statement (see
[Security](../ops/security.md)). There is no metrics endpoint yet; the server
logs to stderr.
