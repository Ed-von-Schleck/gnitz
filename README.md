# Gnitz

`Gnitz` is a SQL database. What makes it different from most other SQL databases is that `VIEW`s are all _materialized and incrementally updated_, meaning: They never go stale, and they are updated _efficiently_ – the cost to update a `VIEW` is proportional to the size of the _changed_ data, regardless of how much _existing_ data there is. It intentionally restricts `SELECT` statements to reading one table or view, so that any query that derives something new is a `VIEW` the engine maintains. `Gnitz` spills these `VIEW`s to disk, so it trades disk-space for latency.

Gnitz guarantees efficient `VIEW` updates by implementing the **DBSP** formal model of incremental computation, treating all data as **Z-Sets** – multisets where every record has an associated integer weight, enabling algebraic coalescing of updates.

Gnitz is pre-alpha.

**Documentation: <https://ed-von-schleck.github.io/gnitz/>**

## Try it

Install the shell, `gnitz`, and the server, `gnitz-server`:

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/gnitz-installer.sh | sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/gnitz-server-installer.sh | sh
gnitz
```

Given no server to connect to, `gnitz` runs `gnitz-server` for the session, and its
data is deleted when the shell exits; `gnitz --data DIR` keeps it in `DIR`. A view
is created once and then kept up to date with every change to what it reads:

```sql
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT);
CREATE VIEW revenue AS SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer;
INSERT INTO orders VALUES (1, 'ann', 30), (2, 'bob', 12), (3, 'ann', 5);
SELECT * FROM revenue ORDER BY customer;
DELETE FROM orders WHERE id = 1;
SELECT * FROM revenue ORDER BY customer;
```

The server runs on Linux 6.7 or later, on x86-64 with AVX2 or on aarch64; the
shell also runs on macOS.

To connect to a running server, give `gnitz` its Unix socket path or a
`tls://host:port` target, or set `GNITZ_TARGET`.

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
```

The client needs Python 3.14 on Linux. `gnitz` is the client alone; the
`server` extra adds `gnitz-server`, which `gnitz.local_server()` runs for
development and tests. Elsewhere, `gnitz.connect()` with no argument connects
to `GNITZ_TARGET`.

For tests, `pytest_plugins = ["gnitz.testing"]` in a `conftest.py` gives the
fixtures `gnitz_server`, `gnitz_target` and `gnitz_client`, the last a
connection in a schema of each test's own. For a FastAPI app, see
[fastapi-gnitz](https://github.com/gnitzlabor/fastapi-gnitz).

## Running a server

[Deployment](https://ed-von-schleck.github.io/gnitz/ops/deployment.html) covers
systemd, containers and Docker Compose;
[Security](https://ed-von-schleck.github.io/gnitz/ops/security.html) TLS and
access tokens; and [Backup and upgrades](https://ed-von-schleck.github.io/gnitz/ops/backup.html)
moving data between releases. A client and a server must be of the same release.

## From source

`cargo install gnitz` builds the shell, and `cargo install gnitz-server` the
server. They build for the baseline of the machine's architecture, where the
release binaries are built for x86-64-v3 (AVX2); `RUSTFLAGS="-C
target-cpu=native"` builds for the machine itself.

To work on gnitz itself you need Linux 6.7 or later, stable Rust and
[`uv`](https://docs.astral.sh/uv/); `make help` lists the targets, and
`make verify` is the gate before a commit. On macOS, work in a Linux VM such as
[Lima](https://lima-vm.io).
