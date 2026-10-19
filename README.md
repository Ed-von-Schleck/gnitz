# Gnitz

`Gnitz` is a SQL database. What makes it different from most other SQL databases is that `VIEW`s are all _materialized and incrementally updated_, meaning: They never go stale, and they are updated _efficiently_ – the cost to update a `VIEW` is always proportional to the size of the _changed_ data, regardless of how much _existing_ data there is. It intentionally restricts `SELECT` statements to the bare minimum so that queries are _always fast, no footguns_. `Gnitz` spills these `VIEW`s to disk, so it trades disk-space for latency.

A Gnitz guarantees efficient `VIEW` updates by implementing the **DBSP** (Differential Dataflow) formal model, treating all data as **Z-Sets** – multisets where every record has an associated integer weight, enabling algebraic coalescing of updates.

Gnitz is pre-alpha.

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
shell also runs on macOS. On macOS run the server in a container, below; on
Windows, install both inside WSL 2.

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

## In a container

The image is `ghcr.io/ed-von-schleck/gnitz-server`. The server runs on io_uring,
which a container runtime's default seccomp profile denies, so it runs under the
profile each release carries. On a laptop, with the port published on
loopback only:

```sh
curl -LO https://github.com/Ed-von-Schleck/gnitz/releases/latest/download/seccomp.json
docker run -d --name gnitz --security-opt seccomp=seccomp.json \
    -v gnitz:/data -p 127.0.0.1:7543:7543 ghcr.io/ed-von-schleck/gnitz-server \
    --tls-listen=0.0.0.0:7543 --tls-name=localhost --allow-unauthenticated /data /data/gnitz.sock
docker cp gnitz:/data/tls_cert.pem gnitz-ca.pem
gnitz "tls://localhost:7543?ca=gnitz-ca.pem"
```

Docker Desktop 4.42 blocks io_uring whatever the profile; the versions after it
do not. [`container/README.md`](container/README.md) is the deployment guide:
access tokens, Docker Compose and Kubernetes.

## In production

On a Linux host, the server's release archive carries `gnitz-server`, the
systemd unit `gnitz-server.service`, whose header says how to install it, and
`seccomp.json`. For containers, see [`container/README.md`](container/README.md).

A client and a server must be of the same release. A client that dials another
release's server is refused, naming both releases.

## From source

`cargo install gnitz` builds the shell, and `cargo install gnitz-server` the
server. They build for the baseline of the machine's architecture, where the
release binaries are built for x86-64-v3 (AVX2); `RUSTFLAGS="-C
target-cpu=native"` builds for the machine itself.

To work on gnitz itself you need Linux 6.7 or later, stable Rust and
[`uv`](https://docs.astral.sh/uv/); `make help` lists the targets, and
`make verify` is the gate before a commit. On macOS, work in a Linux VM such as
[Lima](https://lima-vm.io).
