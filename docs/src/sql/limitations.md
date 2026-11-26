# Limitations

What gnitz refuses today that no other page covers, and what to do instead.
The planner refuses each of these with an error naming it.

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

**The same name in a query and its subquery.** The planner does not let an
inner name shadow an outer one: give the same relation an alias in at least one
of them, and qualify a column name both of them have.

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
- **No non-deterministic functions** (`RANDOM()`, `CURRENT_DATE`, …) other
  than `NOW()` in a view's `WHERE`. Generate random values in the client.
- **128-bit columns** (`UUID`, `HUGEINT`, `UHUGEINT`) can be stored, selected,
  grouped by, joined on and used as keys and indexed, but not used in an
  expression. A plain `SELECT` still compares one with a value through its
  primary key or an index: an equality, a range or an `IN` list there reads the
  key, and needs no expression.

```sql
CREATE TABLE devices (id UUID PRIMARY KEY, owner UUID, label TEXT);
CREATE INDEX devices_by_owner ON devices (owner);
SELECT label FROM devices WHERE id = '6f1c1a8e-3b8e-4c47-9d1e-2f0b6f9a1c2d';
SELECT label FROM devices WHERE owner = '0b9e7f3a-1c2d-4e5f-8a9b-0c1d2e3f4a5b';
SELECT label FROM devices
    WHERE owner IN ('0b9e7f3a-1c2d-4e5f-8a9b-0c1d2e3f4a5b', '6f1c1a8e-3b8e-4c47-9d1e-2f0b6f9a1c2d');
```

```sql,refused
CREATE VIEW mine AS
    SELECT label FROM devices WHERE owner = '0b9e7f3a-1c2d-4e5f-8a9b-0c1d2e3f4a5b';
```

```text
column "owner" is UUID; 128-bit columns cannot be used in expressions (a plain SELECT compares one with a value through its primary key or an index)
```

## Writes

**`TRUNCATE`** is not supported: `DELETE FROM t`.

## Operations

gnitz is a single server: there is no replication or failover. Access control
is all-or-nothing: an authenticated client may run any statement (see
[Security](../ops/security.md)). There is no metrics endpoint; the master logs
to stderr, and each worker to a file in the data directory.
