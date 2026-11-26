# Tables, views and streams

gnitz has three kinds of relation.

| | Holds rows | Written by | Read by `SELECT` | Read in a view body |
|---|---|---|---|---|
| **Table** | yes | `INSERT`, `UPDATE`, `DELETE`, client pushes | yes | yes |
| **View** | yes, maintained | the engine | yes | yes |
| **Stream** | no | `INSERT`, client pushes | no | yes |

## Tables

A table is what you would expect: rows with a primary key, written by
`INSERT`, `UPDATE` and `DELETE`, durable once a write is acknowledged.

## Views

Every view is materialized. `CREATE VIEW` computes the view once from what its
sources hold, and from then on each write to a source is turned into the change
it makes to the view, which is applied to the view's stored rows. Reading a view
reads those rows.

A view may read tables, streams and other views. Its cost is paid at write
time, in proportion to the size of the change; a write that touches one row of
a large table costs about as much as a write to a small one. In return the
engine keeps state for a view — a join keeps both of its inputs, an aggregate
its groups — so views cost disk space.

A view can be created before or after its sources hold data. Creating it over
existing data computes it from that data first.

## Plain `SELECT`

A plain `SELECT` reads one table or view: it can filter, group, aggregate, sort
and limit, but it does not join or nest. The idea is that a query worth asking
repeatedly is worth maintaining:

```sql
CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT, region TEXT);
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer BIGINT, amount BIGINT);
CREATE VIEW revenue_by_region AS
    SELECT c.region, SUM(o.amount) AS total
    FROM orders o JOIN customers c ON o.customer = c.id
    GROUP BY c.region;
SELECT * FROM revenue_by_region WHERE total > 100;
```

The same join as a plain `SELECT` is refused, with a message that says to make
it a view:

```sql,refused
SELECT * FROM orders o JOIN customers c ON o.customer = c.id;
```

```text
ad-hoc SELECT reads a single relation; this query derives a new one (JOIN).
CREATE VIEW <name> AS <your query> — the engine maintains it incrementally — then SELECT from it.
```

[Reading data](../sql/reads.md) has the details.

## Streams

A stream is a table that keeps no rows. Rows written to it reach every view
that reads it and are then gone, so it suits ingestion where only the derived
views are worth keeping — events, logs, metrics:

```sql
CREATE TABLE events (id BIGINT PRIMARY KEY, kind BIGINT, amount BIGINT)
    WITH (stream = true);
CREATE VIEW totals AS SELECT kind, SUM(amount) AS total FROM events GROUP BY kind;
INSERT INTO events VALUES (1, 1, 10), (2, 1, 5), (3, 2, 7);
SELECT * FROM totals;
```

```sql,refused
SELECT * FROM events;
```

```text
'events' is a stream; SELECT requires a table or a view (a stream is read only inside a view body)
```

A stream differs from a table in three more ways:

- **It is append-only.** It takes `INSERT` and pushes, but no `UPDATE`,
  `DELETE`, index, foreign key or transactional write.
- **Its key is not unique.** Inserting the same row twice counts it twice.
- **It is not recovered.** A write to a stream is not made durable, and at
  every start of the server each view reading a stream is rebuilt as if the
  stream had never received a row. A view `totals` like the one above starts
  empty again after a restart.

## The catalog

The schema `_system` holds the catalog as ordinary relations: `schemas`,
`tables`, `views`, `columns`, `indices`, `circuits` and `sequences`. A `SELECT`
reads them like any table, and a view may read them too:

```sql
SELECT name, stream FROM _system.tables WHERE schema_id > 1;
```
