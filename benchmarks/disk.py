"""Disk footprint of a string-heavy table and the views over it.

Loads `events` — four TEXT columns drawn from small value sets, so every value
repeats many times — under a filter view that copies its strings and a grouped
view keyed on two of them, shuts the server down so its final checkpoint puts
every store on disk, and prints `gnitz-server --disk-usage` for the data
directory: bytes by relation, store, LSM level and shard region.

The server sizes a store's RAM tier at 32 MiB, and a store that never fills it
is one L0 shard per worker at the checkpoint. `--ram-tier-bytes` shrinks the
tier so the same rows spill, fold and compact into the deeper levels.

Run through `make bench-disk`, which builds the server and the extension first.
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "crates/gnitz-py/tests"))

import gnitz  # noqa: E402
from _serverproc import ServerProc, disk_usage  # noqa: E402

TENANTS = [f"tenant-{i:03d}" for i in range(50)]                    # inline: 12 bytes or fewer
STATUSES = ["ok", "ok", "ok", "client_error", "server_error_upstream_timeout"]
URL_VALUES = 2_000
AGENT_VALUES = 30

DDL = [
    "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, tenant TEXT NOT NULL, "
    "url TEXT NOT NULL, agent TEXT NOT NULL, status TEXT NOT NULL, amount BIGINT NOT NULL)",
    "CREATE VIEW v_errors AS SELECT id, tenant, url, agent, status FROM events WHERE status <> 'ok'",
    "CREATE VIEW v_by_url AS SELECT tenant, url, COUNT(*) AS n, SUM(amount) AS total "
    "FROM events GROUP BY tenant, url",
]
RELATIONS = ["events", "v_errors", "v_by_url"]


def load(conn, rows, batch_rows):
    """Push `rows` events; returns the bytes of the values pushed."""
    rng = random.Random(7)
    urls = [f"https://app.example.com/api/v2/resources/{rng.randrange(10**9):09d}/items/{i:05d}"
            for i in range(URL_VALUES)]
    agents = [f"Mozilla/5.0 (X11; Linux x86_64; rv:{100 + i}.0) Gecko/20100101 Firefox/{100 + i}.0 "
              f"build-{i:04d}" for i in range(AGENT_VALUES)]
    # Views first: a view maintains only the deltas pushed after it exists.
    for stmt in DDL:
        conn.execute_sql(stmt)
    tid, schema = conn.resolve_table("events")
    logical = 0
    for start in range(0, rows, batch_rows):
        batch = gnitz.ZSetBatch(schema)
        for k in range(start, min(start + batch_rows, rows)):
            tenant, url, agent, status = (rng.choice(TENANTS), rng.choice(urls), rng.choice(agents),
                                          rng.choice(STATUSES))
            logical += 16 + len(tenant) + len(url) + len(agent) + len(status)
            batch.append(id=k + 1, tenant=tenant, url=url, agent=agent, status=status,
                         amount=rng.randrange(1000))
        conn.push(tid, batch)
    return logical


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--rows", type=int, default=400_000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--batch-rows", type=int, default=20_000)
    ap.add_argument("--ram-tier-bytes", type=int, default=None,
                    help="GNITZ_RAM_TIER_BYTES for the server (default: the server's own)")
    ap.add_argument("--keep", action="store_true", help="keep the data directory and print its path")
    args = ap.parse_args()

    (REPO_ROOT / "tmp").mkdir(exist_ok=True)
    tmp = Path(tempfile.mkdtemp(dir=REPO_ROOT / "tmp", prefix="bench_disk_"))
    data_dir = tmp / "data"
    server = ServerProc(str(data_dir), str(tmp / "gnitz.sock"))
    # The SAL is fallocated whole at boot and is not what this measures.
    server.extra_env["GNITZ_SAL_BYTES"] = os.environ.get("GNITZ_SAL_BYTES", str(256 << 20))
    if args.ram_tier_bytes is not None:
        server.extra_env["GNITZ_RAM_TIER_BYTES"] = str(args.ram_tier_bytes)
    server.start(workers=args.workers, timeout=20.0)
    try:
        with gnitz.connect(server.sock_path) as conn:
            logical = load(conn, args.rows, args.batch_rows)
            names = {}
            for name in RELATIONS:
                tid, schema = conn.resolve_table(name)
                names[tid] = (name, len(conn.scan(tid, schema)))
        # Its final checkpoint puts every store on disk.
        server.stop_graceful(timeout=300)
    finally:
        server.stop()

    print(f"{args.rows} events of {logical} value bytes ({logical / args.rows:.1f} per row), "
          f"{args.workers} workers, RAM tier {args.ram_tier_bytes or 'default'}")
    for tid, (name, live) in sorted(names.items()):
        print(f"  relation {tid} = {name}: {live} rows")
    report, stores = disk_usage(data_dir)
    # The system relations hold a few rows each.
    system = {s["line"] for s in stores if s["relation"] < gnitz.FIRST_USER_TABLE_ID}
    print("\n".join(line for line in report.splitlines() if line not in system))
    shard_bytes = sum(s["bytes"] for s in stores if s["line"] not in system)
    print(f"shard bytes per value byte: {shard_bytes / logical:.2f}")
    if args.keep:
        print(f"kept: {tmp}")
    else:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
