# Getting started

## Install

gnitz is two programs: the shell, `gnitz`, and the server, `gnitz-server`.

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/gnitz-installer.sh | sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/gnitz-server-installer.sh | sh
```

The server runs on Linux 6.7 or later, on x86-64 with AVX2 or on aarch64.

## A first view

Start the shell:

```sh
gnitz
```

Given no server to connect to — no target argument and no `GNITZ_TARGET` — the shell runs a `gnitz-server` for the session
and deletes its data when you exit. `gnitz --data DIR` keeps the data in `DIR`
for the next session.

Create a table and a view over it:

```sql
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT);
CREATE VIEW revenue AS
    SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer;
```

The view exists before any data does. Every write to `orders` from now on
updates `revenue`:

```sql
INSERT INTO orders VALUES (1, 'ann', 30), (2, 'bob', 12), (3, 'ann', 5);
SELECT * FROM revenue ORDER BY customer;
```

```text
╭──────────┬───────╮
│ customer ┆ total │
╞══════════╪═══════╡
│ ann      ┆    35 │
│ bob      ┆    12 │
╰──────────┴───────╯
(2 rows)
```

```sql
DELETE FROM orders WHERE id = 1;
SELECT * FROM revenue ORDER BY customer;
```

```text
╭──────────┬───────╮
│ customer ┆ total │
╞══════════╪═══════╡
│ ann      ┆     5 │
│ bob      ┆    12 │
╰──────────┴───────╯
(2 rows)
```

The delete did not recompute the sum over `orders`; it subtracted 30 from
`ann`'s total.

## From Python

```sh
uv add "gnitz[server]"        # or: pip install "gnitz[server]"
```

```python
import gnitz

with gnitz.local_server() as server, gnitz.connect(server.target) as conn:
    conn.execute_sql("CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT)")
    conn.execute_sql("CREATE VIEW revenue AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer")
    conn.execute_sql("INSERT INTO orders VALUES (1, 'ann', 30), (2, 'bob', 12)")
    [result] = conn.execute_sql("SELECT * FROM revenue")
    for row in result["rows"]:
        print(row.customer, row.total)
```

`gnitz.local_server()` runs a server for development and tests; see
[Python](clients/python.md) for connecting to a running one.

## Next

- [Tables, views and streams](concepts/relations.md) — the model.
- [Views](sql/views.md) — what a view body can contain.
- [Deployment](ops/deployment.md) — running a server that keeps its data.
