# Reading data

```sql
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer BIGINT, amount BIGINT, note TEXT);
INSERT INTO orders VALUES (1, 1, 5, 'gift'), (2, 1, 7, NULL), (3, 2, 1, NULL);
```

A plain `SELECT` reads **one** table, view or catalog relation:

```sql
SELECT * FROM orders WHERE id = 2;
SELECT id, amount * 2 AS doubled, COALESCE(note, '-') AS note FROM orders WHERE amount > 1;
SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer HAVING SUM(amount) > 5;
SELECT DISTINCT customer FROM orders;
SELECT * FROM orders ORDER BY amount DESC LIMIT 2 OFFSET 1;
```

It takes `WHERE`, expressions, `GROUP BY` with `HAVING` and aggregates,
`DISTINCT`, `ORDER BY`, `LIMIT` and `OFFSET`. Joins, set operations,
subqueries, window functions and aggregates over a `DISTINCT` are refused, with
a message saying to put the query in a [view](views.md):

```sql,refused
SELECT * FROM orders o WHERE customer IN (SELECT customer FROM orders big WHERE big.amount > 6);
```

```text
WHERE: ad-hoc SELECT reads a single relation; this query derives a new one (EXISTS/IN subquery).
CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it.
```

```sql
CREATE VIEW big_spenders AS
    SELECT * FROM orders o WHERE customer IN (SELECT customer FROM orders big WHERE big.amount > 6);
SELECT * FROM big_spenders;
```

Rows come back in no particular order unless the query has `ORDER BY`.

## Consistency

A read sees every write acknowledged before it, including in views: a read of a
view first applies any changes to its sources that have not reached it yet.

## How a read finds its rows

A `WHERE` naming the whole primary key reads that one row; a condition on a
leading part of a compound key reads a range; an equality or a range on an
[indexed](tables.md#indexes) column reads through the index. Anything else
scans the relation. `EXPLAIN` shows which:

```sql
EXPLAIN SELECT * FROM orders WHERE id = 2;
```

```text
read table orders
access: pk point lookup
predicate: none
projection: 4 columns (unprojected)
order/limit: none
```

A query asked often over a large relation is better served by a view that
holds its answer than by a scan.
