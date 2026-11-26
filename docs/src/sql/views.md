# Views

```sql
CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL, region TEXT);
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer BIGINT NOT NULL, amount BIGINT);
CREATE VIEW big_orders AS SELECT id, customer, amount FROM orders WHERE amount > 100;
```

A view is materialized when it is created and kept up to date by every write
to what it reads; see [Tables, views and streams](../concepts/relations.md#views).
A view may read tables, streams, other views and the `_system` catalog.

```sql
CREATE OR REPLACE VIEW big_orders AS SELECT id, customer, amount FROM orders WHERE amount > 500;
ALTER VIEW big_orders AS SELECT id, amount FROM orders WHERE amount > 500;
DROP VIEW big_orders;
```

## What a view body can contain

**Filters and projections** — `WHERE`, expressions, `CASE`, casts and the
[functions](functions.md).

**Joins** — `INNER`, `LEFT`, `RIGHT` and `FULL` joins on equality, `INNER` and
`LEFT` joins on a range alone (`<`, `<=`, `>`, `>=`), cross joins and
self-joins:

```sql
CREATE VIEW order_names AS
    SELECT o.id, c.name, o.amount
    FROM orders o JOIN customers c ON o.customer = c.id;
CREATE VIEW customers_and_orders AS
    SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer = c.id;
```

**Aggregation** — `GROUP BY` and `HAVING` with `COUNT`, `SUM`, `MIN`, `MAX`
and `AVG`, each also as `DISTINCT`:

```sql
CREATE VIEW per_customer AS
    SELECT customer, COUNT(*) AS n, SUM(amount) AS total, MAX(amount) AS largest
    FROM orders GROUP BY customer HAVING COUNT(*) > 1;
CREATE VIEW amounts AS
    SELECT customer, COUNT(DISTINCT amount) AS distinct_amounts, SUM(amount) AS total
    FROM orders GROUP BY customer;
```

**Set operations** — `UNION`, `INTERSECT` and `EXCEPT`, each with or without
`ALL`, and `SELECT DISTINCT`:

```sql
CREATE VIEW idle_customers AS
    SELECT id FROM customers EXCEPT SELECT customer FROM orders;
```

**Subqueries** — `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, `ANY`/`ALL`
comparisons, derived tables and `WITH` (not recursive). A scalar subquery is
accepted when it is one aggregate, over the whole relation or over the rows
correlated with the outer row:

```sql
CREATE VIEW customers_without_orders AS
    SELECT id, name FROM customers c
    WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer = c.id);
CREATE VIEW customer_totals AS
    SELECT id, name, (SELECT SUM(amount) FROM orders o WHERE o.customer = c.id) AS total
    FROM customers c;
```

**Top-N** — `ORDER BY … LIMIT n [OFFSET m]` keeps the first `n` rows in that
order. A view is an unordered set, so `ORDER BY` without `LIMIT` is refused;
order when you read instead.

```sql
CREATE VIEW top_orders AS SELECT id, amount FROM orders ORDER BY amount DESC LIMIT 10;
```

**Window functions** — `ROW_NUMBER`, `RANK` and `DENSE_RANK`, and `COUNT`,
`SUM`, `MIN`, `MAX` and `AVG` over a window, with `PARTITION BY`, `ORDER BY`,
named `WINDOW`s and `QUALIFY`:

```sql
CREATE VIEW largest_per_customer AS
    SELECT id, customer, amount FROM orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount DESC) = 1;
CREATE VIEW running_total AS
    SELECT id, customer, SUM(amount) OVER (PARTITION BY customer ORDER BY id) AS so_far
    FROM orders;
```

A window with an `ORDER BY` costs, per change, a pass over the changed row's
partition — the rows before it are read and those after it are emitted again —
so keep partitions small where rows change often. A `QUALIFY` that bounds a
`ROW_NUMBER` nothing else reads is the exception: it keeps the top rows of each
partition instead.
An aggregate over a window with no `ORDER BY` needs its `PARTITION BY` columns
to be `NOT NULL`.

**The current time** — `NOW()` (or `CURRENT_TIMESTAMP`) is allowed in a view's
`WHERE`, compared with an expression over one relation's columns. The view moves
as time passes: rows enter and leave it as the clock crosses their bound. With
no `INTERVAL` type, a duration is written in microseconds:

```sql
CREATE TABLE sessions (id BIGINT PRIMARY KEY, last_seen TIMESTAMP NOT NULL);
CREATE VIEW active_sessions AS
    SELECT id FROM sessions WHERE last_seen > NOW() - 900000000;  -- 15 minutes
```

What is refused is listed under [Limitations](limitations.md).

## Options

`CREATE VIEW name WITH (…) AS …` takes:

| Option | Effect |
|---|---|
| `delta = '<size>'` | Keep the view's recent changes, up to the size, as a [delta feed](#delta-feeds). |
| `capacity = '<size>'` | Bound the view's stored rows on each worker; see below. |
| `compress = true` | The view's text and byte values are compressed on disk, where that saves space. |

Sizes are written `'512 KB'`, `'32 MB'`, `'1 GB'`.

### Delta feeds

A view with `delta` keeps its recent changes so a client can follow them
instead of re-reading the view: each change arrives as rows with weights,
`+1` for a row added and `-1` for one removed. The shell's
[`\watch`](../clients/shell.md#watching-a-view) prints them.

```sql
CREATE VIEW order_feed WITH (delta = '32 MB') AS SELECT id, customer, amount FROM orders;
```

The feed keeps changes up to its size, and only until the server restarts. A
client that falls further behind is told so, and must read the whole view
again.

### Capacity

A view with `capacity` keeps at most that many bytes of rows per worker in
full; past it, rows are stored as keys only and read back by recomputing them
from the view's sources. The view stays exact; reads of evicted rows cost more.
Nothing can be built on a capacity-bounded view, and it cannot be indexed.

```sql
CREATE VIEW recent_orders WITH (capacity = '4 MB') AS
    SELECT id, amount FROM orders WHERE amount > 0;
```
