"""Deterministic data generators and bulk-load helpers for benchmarks."""

from __future__ import annotations

import bisect
import random

import gnitz

# ---------------------------------------------------------------------------
# Scale models
# ---------------------------------------------------------------------------

SCALES = {
    "quick": {
        "rows": 1_000,
        "insert_batches": 10,
        "read_iters": 20,
        "write_iters": 10,
        "delta_rows": 50,
    },
    "full": {
        "rows": 150_000,
        "insert_batches": 1500,
        "read_iters": 50,
        "write_iters": 50,
        "delta_rows": 100,
    },
}

# Per-feature incremental-maintenance sizing (features/ tier). `full` pushes the
# base past the ephemeral-store ceiling so view stores are disk-resident.
FEATURE_SIZES = {
    "quick": {"dim": 2_000, "base": 20_000, "delta": 200, "iters": 20},
    "full": {"dim": 200_000, "base": 1_000_000, "delta": 4_000, "iters": 50},
}


def feature_sz(scale_mode):
    """The feature-tier row count for the active scale (`quick`/`full`)."""
    return FEATURE_SIZES[scale_mode]

# TPC-H-subset sizing (combined/test_tpch.py). `sf` scales the base; `delta` is
# the streamed rows/iter; `skew_s` is the Zipfian exponent on hot FKs.
TPCH_SIZES = {
    "quick": {"sf": 0.5, "delta": 2_000, "iters": 20, "skew_s": 1.1},
    "full": {"sf": 10, "delta": 2_000, "iters": 50, "skew_s": 1.1},
}

# Transaction sizing (txn/ tier). Contention sweep knobs live alongside.
TXN_SIZES = {
    "quick": {"ops": 200, "K_lines": 4},
    "full": {"ops": 5_000, "K_lines": 8},
}
OCC_CLIENTS = [1, 2, 4]      # N: concurrent RMW clients
OCC_HOT_SET = [1, 8, 64]     # H: hot-row set size (write-set per txn)

# RYOW serving-path sizing (combined/test_serving.py).
SERVING_SIZES = {
    "quick": {"groups": 1_000, "writes": 2_000, "seeks": 2_000},
    "full": {"groups": 100_000, "writes": 50_000, "seeks": 50_000},
}

# HTAP sizing (combined/test_htap.py): concurrent writers + dashboard readers.
HTAP_SIZES = {
    "quick": {"duration_s": 3.0, "writers": 2, "readers": 2, "K_lines": 4,
              "products": 500, "base_orders": 2_000},
    "full": {"duration_s": 10.0, "writers": 2, "readers": 2, "K_lines": 8,
             "products": 5_000, "base_orders": 20_000},
}

# ---------------------------------------------------------------------------
# String pools (all <=12 chars to stay inline in German Strings)
# ---------------------------------------------------------------------------

NATIONS = [
    "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE",
    "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
    "MOROCCO", "PERU", "CHINA", "ROMANIA", "S_ARABIA", "VIETNAM", "RUSSIA",
    "UK", "USA", "MEXICO",
]
STATUS = ["F", "O", "P"]
SHIPMODES = ["AIR", "SHIP", "MAIL", "RAIL", "TRUCK"]


def NAME(prefix: str, i: int) -> str:
    return f"{prefix}#{i}"


# ---------------------------------------------------------------------------
# Zipfian key selection (hot-customer / hot-product skew)
# ---------------------------------------------------------------------------

_zipf_cache: dict[tuple[int, float], list[float]] = {}


def _zipf_cdf(n: int, s: float) -> list[float]:
    key = (n, s)
    cdf = _zipf_cache.get(key)
    if cdf is None:
        weights = [1.0 / (k ** s) for k in range(1, n + 1)]
        total = sum(weights)
        cum = 0.0
        cdf = []
        for w in weights:
            cum += w / total
            cdf.append(cum)
        _zipf_cache[key] = cdf
    return cdf


def zipf_choice(rng: random.Random, n: int, s: float = 1.1) -> int:
    """1-based key in [1, n] drawn from a cached Zipfian(s) CDF."""
    cdf = _zipf_cdf(n, s)
    return bisect.bisect_left(cdf, rng.random()) + 1


# ---------------------------------------------------------------------------
# SQL INSERT generator (micro/ SQL-path latency benches)
# ---------------------------------------------------------------------------


class DataGen:
    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)

    def insert_sql(
        self,
        table: str,
        cols: list[str],
        batch_size: int,
        batch_idx: int,
        col_ranges: dict[str, tuple[int, int]] | None = None,
    ) -> str:
        """Generate INSERT INTO t VALUES (...), ... with deterministic values.

        PKs are monotonic (batch_idx * batch_size + offset) to avoid conflicts.
        col_ranges: optional {col_name: (lo, hi)} to constrain specific columns.
        """
        base_pk = batch_idx * batch_size
        rows = []
        for off in range(batch_size):
            pk = base_pk + off + 1  # 1-based PKs
            vals = [str(pk)]
            for c in cols[1:]:  # skip pk column
                if col_ranges and c in col_ranges:
                    lo, hi = col_ranges[c]
                    vals.append(str(self._rng.randint(lo, hi)))
                else:
                    vals.append(str(self._rng.randint(0, PAYLOAD_MAX)))
            rows.append(f"({', '.join(vals)})")
        col_list = ", ".join(cols)
        return f"INSERT INTO {table} ({col_list}) VALUES {', '.join(rows)}"

    def insert_stmts(
        self, table: str, cols: list[str], batch_size: int, num_batches: int
    ) -> list[str]:
        return [
            self.insert_sql(table, cols, batch_size, i)
            for i in range(num_batches)
        ]

    def update_sql(
        self, table: str, set_col: str, set_val: int, pk: int
    ) -> str:
        return f"UPDATE {table} SET {set_col} = {set_val} WHERE pk = {pk}"

    def delete_sql(self, table: str, pk: int) -> str:
        return f"DELETE FROM {table} WHERE pk = {pk}"


# ---------------------------------------------------------------------------
# push() value generation
# ---------------------------------------------------------------------------

PAYLOAD_MAX = 1_000_000

_NARROW = {
    gnitz.TypeCode.I8: (-100, 100),
    gnitz.TypeCode.U8: (0, 200),
    gnitz.TypeCode.I16: (-30_000, 30_000),
    gnitz.TypeCode.U16: (0, 60_000),
    gnitz.TypeCode.I32: (-1_000_000, 1_000_000),
    gnitz.TypeCode.U32: (0, 2_000_000),
}


def gen_value(rng, col, i, pools=None, skew=None):
    """A single deterministic value for a payload/FK column of `col`.

    pools: {col_name: [str, ...]} for STRING columns (default: NATIONS).
    skew: {col_name: (n, s)} draws that column via zipf_choice in [1, n].
    """
    name = col.name
    tc = col.type_code
    if skew and name in skew:
        n, s = skew[name]
        return zipf_choice(rng, n, s)
    if tc == gnitz.TypeCode.STRING:
        pool = (pools or {}).get(name, NATIONS)
        return rng.choice(pool)
    if tc in (gnitz.TypeCode.F64, gnitz.TypeCode.F32):
        return rng.random() * 1_000_000
    if tc in (gnitz.TypeCode.U128, gnitz.TypeCode.UUID, gnitz.TypeCode.I128):
        return rng.getrandbits(120)
    if tc in _NARROW:
        lo, hi = _NARROW[tc]
        return rng.randint(lo, hi)
    return rng.randint(0, PAYLOAD_MAX)


def bulk_load(
    conn,
    schema_name: str,
    table_name: str,
    columns: list,
    num_rows: int,
    seed: int = 42,
    *,
    pools=None,
    skew=None,
    null_rate: float = 0.1,
    chunk: int = 250_000,
) -> list[int]:
    """Bulk load via push() API (ZSetBatch). Returns list of PKs loaded.

    Honors STRING/float/narrow types, nullable columns (emit None at
    `null_rate`), and a `skew` {col: (n, s)} FK-selection map.
    """
    tid, schema = conn.resolve_table(schema_name, table_name)
    rng = random.Random(seed)
    # Hoisted: `c.type_code` crosses into Rust on every read, and the tuple
    # unpack below is one bytecode where the attribute walk is three.
    spec = [(c.name, getattr(c, "is_nullable", False), c) for c in columns if c.name != "pk"]
    pks = []
    i = 1

    def rows(lo, hi):
        # A generator, not a list: `extend` iterates it, so one row dict is alive
        # at a time where a materialized list would hold the whole chunk.
        for k in range(lo, hi):
            row = {"pk": k}
            for name, nullable, col in spec:
                if nullable and rng.random() < null_rate:
                    row[name] = None
                else:
                    row[name] = gen_value(rng, col, k, pools, skew)
            yield row

    while i <= num_rows:
        end = min(i + chunk, num_rows + 1)
        # `extend`, not a per-row `append(**row)`: a `**` splat makes CPython
        # build a fresh keyword tuple per call, and `extend` consumes the dict
        # this loop already builds directly.
        conn.push(tid, gnitz.ZSetBatch(schema).extend(rows(i, end)))
        pks.extend(range(i, end))
        i = end
    return pks


def build_batch(schema, rows, weight: int = 1):
    """Build a ZSetBatch from a list of row dicts, all at `weight` (no push).

    Callers that time only the push (bench_timer.measure) build the batch with
    this first, then measure conn.push(tid, batch) alone.
    """
    return gnitz.ZSetBatch(schema).extend(rows, weight)


def push_rows(conn, tid, schema, rows, weight: int = 1) -> None:
    """Push a ready list of row dicts as one ZSetBatch at `weight`."""
    conn.push(tid, build_batch(schema, rows, weight))


def push_one(conn, tid, schema, **row) -> None:
    """Push a single row (weight=1) as its own batch."""
    conn.push(tid, gnitz.ZSetBatch(schema).extend([row]))


def push_stream(client, tid, schema, build, count, chunk: int = 250_000) -> None:
    """Push `count` rows via push() in `chunk`-sized batches.

    build(batch, k) appends one row for 0-based index k. This is the chunked
    delta driver shared by the features/ and combined/ streaming benches; each
    chunk > the row-coalesce threshold trips an auto-tick.
    """
    k = 0
    while k < count:
        batch = gnitz.ZSetBatch(schema)
        end = min(k + chunk, count)
        for j in range(k, end):
            build(batch, j)
        client.push(tid, batch)
        k = end


def stream_deltas(
    client,
    bench_timer,
    tid,
    schema,
    build,
    iters: int,
    delta: int,
    *,
    retract: bool = True,
    retract_lag: int = 4,
) -> int:
    """Stream `iters` push() delta epochs into `tid`, timing each push.

    build(batch, epoch, j, weight) appends row j of `epoch` at the given weight;
    it MUST be deterministic in (epoch, j) so a retraction regenerates the exact
    (PK, payload). Each epoch pushes `delta` fresh inserts (measured); after
    `retract_lag` epochs it also pushes a retract batch (weight=-1) of an earlier
    epoch — exercising both the additive and non-linear/clamp delta paths.
    Deltas hit exactly one relation per epoch (single-source-per-epoch).

    Returns the total rows streamed (measured epochs only, via bench_timer).
    """
    total = 0
    for e in range(iters):
        b = gnitz.ZSetBatch(schema)
        for j in range(delta):
            build(b, e, j, 1)
        bench_timer.measure(client.push, tid, b, rows_per_call=delta)
        total += delta
        if retract and e >= retract_lag:
            rb = gnitz.ZSetBatch(schema)
            re = e - retract_lag
            for j in range(delta):
                build(rb, re, j, -1)
            bench_timer.measure(client.push, tid, rb, rows_per_call=delta)
            total += delta
    return total


# ---------------------------------------------------------------------------
# Streaming-delta scaffolding (features/ tier)
# ---------------------------------------------------------------------------

# Streamed PKs live above every base PK; each epoch strides by STRIDE (>> delta)
# so an epoch's rows never collide with the base load or with another epoch.
STREAM_BASE = 1_000_000_000
STRIDE = 1_000_000


def seed_stream(row_fn):
    """Build (base_seed, stream) from one `row_fn(batch, pk, weight)`.

    base_seed(batch, k) fills base PK k+1 at weight 1; stream(batch, e, j, w)
    fills streamed PK STREAM_BASE + e*STRIDE + j at weight w. row_fn must be
    deterministic in pk so a retraction regenerates the exact (PK, payload).
    """
    def base(batch, k):
        row_fn(batch, k + 1, 1)

    def stream(batch, e, j, w):
        row_fn(batch, STREAM_BASE + e * STRIDE + j, w)

    return base, stream


def stream_factory(append_row, vals):
    """Build (base, stream) where each row's values come from a per-row RNG.

    vals(rng) -> tuple of the non-PK column values; append_row(batch, pk, vals,
    weight). The base RNG is seeded by the base index, the stream RNG by
    (epoch, j), so a retraction of epoch e regenerates the identical (PK, payload).
    """
    def base(batch, k):
        append_row(batch, k + 1, vals(random.Random(k)), 1)

    def stream(batch, e, j, w):
        append_row(batch, STREAM_BASE + e * STRIDE + j, vals(random.Random(e * 100003 + j)), w)

    return base, stream


def stream_and_assert(client, sn, bench_timer, tid, schema, build, sz, read_view,
                      *, allow_empty=False):
    """Stream `sz["iters"]` delta epochs into (tid, schema), then assert
    `read_view` is non-empty (unless allow_empty). Returns rows streamed."""
    n = stream_deltas(client, bench_timer, tid, schema, build, sz["iters"], sz["delta"])
    vid, _ = client.resolve_table(sn, read_view)
    if not allow_empty:
        assert len(client.scan(vid)) > 0, f"{read_view} empty after streaming {n} rows"
    return n


# ---------------------------------------------------------------------------
# Secondary-index probe rows (micro/ index-seek benches)
# ---------------------------------------------------------------------------

# `val` above every generated payload, so a probe matches only its own row; one
# row per iteration of the longest scale, so no iteration re-probes a warm one.
PROBE_BASE = PAYLOAD_MAX + 1
PROBE_COUNT = max(max(s["read_iters"], s["write_iters"]) for s in SCALES.values())


def seed_index_probes(conn, schema_name: str, table_name: str, first_pk: int) -> None:
    """Push PROBE_COUNT rows into a `(pk, val, cat)` table for `probe_val` to hit."""
    tid, schema = conn.resolve_table(schema_name, table_name)
    push_rows(conn, tid, schema,
              [{"pk": first_pk + i, "val": PROBE_BASE + i, "cat": 0}
               for i in range(PROBE_COUNT)])


def probe_val(i: int) -> int:
    """The `val` of probe row `i`."""
    return PROBE_BASE + i % PROBE_COUNT
