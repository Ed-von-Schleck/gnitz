"""What a scenario is, and the phases a scenario of rows under views runs."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Callable

LOAD_BATCH = 20_000     # a bulk load: every push is past the tick-coalesce threshold
INGEST_BATCH = 1_000    # a stream of deltas whose ticks the server paces

BYTES = ("l0", "compacted", "checkpointed")     # scenarios read for their bytes run every regime

WORDS = [f"{a}{b}{c}" for a in ("in", "con", "re", "de", "trans", "per", "sub", "ex")
         for b in ("struct", "form", "port", "duc", "scrib", "mit", "vers", "clud")
         for c in ("", "ion", "ed", "ing", "or", "ive", "able", "ure")]


def ops_for(run, per=1000, least=200):
    """How many single operations a phase runs."""
    return max(run.rows // per, least)


def scatter(k: int, bits: int = 64) -> int:
    """A fixed pseudo-random value for `k`: a key a rewrite of row `k` finds again."""
    return (k + 1) * 0x9E3779B97F4A7C15F39CC0605CEDC835 % (1 << 128) >> (128 - bits)


@dataclass
class Scenario:
    name: str
    family: str
    doc: str
    # The storage regimes `--regime=every` takes it through.
    regimes: tuple = ("l0",)
    # The rows it loads whatever a run asks for.
    fixed_rows: int | None = field(default=None, kw_only=True)
    # `{view: why}` for a view whose backfill is not its maintained value.
    unverified: dict = field(default_factory=dict, kw_only=True)

    def run(self, run):
        raise NotImplementedError


@dataclass
class Fact:
    """A table the phases keep writing: `make(k)` is row `k`, with a key that is
    a function of `k` alone and a payload drawn afresh on every call. `share`
    is its rows per row of the scenario."""
    make: Callable[[int], dict]
    share: float = 1.0


@dataclass
class Shape(Scenario):
    """Rows of one shape under the views that stress it, through every phase:

    load      `rows` rows in bulk, each push's tick finished before the next
    checkpoint, boot   a graceful stop, the disk reading, and the boot after it
    ingest    half as many again, in batches the server's own ticks pace
    trickle   one-row pushes, each read back at once: an application's write
              unit, so the views' fixed cost per tick is paid per row
    churn     rows rewritten and rows deleted
    retire    with `retire`, each named table's oldest rows deleted oldest first
    recover   a kill, and the boot that replays the log

    With `rescale`, `boot` runs at `rescale` times the run's worker count and
    `recover` back at the run's own, so the stores are relaid onto a wider count
    and the log tail is re-sliced onto a narrower one.
    """

    ddl: list = field(default_factory=list)
    # `(rows, rng) -> (dims, facts)`: `dims` is `[(table, rows)]` loaded once,
    # `facts` is `{table: Fact}`.
    data: Callable = None
    rewrites: float = 0.25      # rows rewritten in `churn`, per loaded row
    deletes: float = 0.05       # rows deleted in `churn`, per loaded row
    # `{table: share}`: the share of the table's rows `retire` deletes, lowest
    # `k` first, which is the order its `make` hands out keys in time. `churn`
    # deletes none of them.
    retire: dict = field(default_factory=dict)
    read: str | None = None     # the view `trickle` reads; the last one by default
    rescale: int = 1

    def run(self, run):
        n = run.rows
        rng = random.Random(7)
        run.ddl(*self.ddl)
        dims, facts = self.data(n, rng)

        def fresh(lo, hi, batch):
            per_table = [run.ops(t, (f.make(k) for k in range(int(lo * f.share), int(hi * f.share))), batch)
                         for t, f in facts.items()]
            return [op for i in range(max(map(len, per_table))) for ops in per_table if i < len(ops) for op in [ops[i]]]

        ops = [op for table, rows in dims for op in run.ops(table, rows, LOAD_BATCH)] + fresh(0, n, LOAD_BATCH)
        # Each bulk push is past the tick threshold; reading after it lets its
        # tick finish before the next arrives, so the ticks are the same every run.
        last = next(reversed(run.views))
        with run.phase("load") as ph:
            ph.replay(ops, read=last)
        home = run.server.workers
        run.restart("loaded", workers=home * self.rescale)

        at = n
        ops = fresh(at, at + n // 2, INGEST_BATCH)
        at += n // 2
        with run.phase("ingest") as ph:
            ph.replay(ops)

        trickled = ops_for(run, 200, 20)
        ops = fresh(at, at + trickled, 1)
        at += trickled
        with run.phase("trickle") as ph:
            ph.replay(ops, read=self.read or last)

        mutable = {t: f for t, f in facts.items() if not run.tables[t].stream}
        assert self.retire.keys() <= mutable.keys(), "retire names a table that is no mutable fact"
        have = {t: int(at * f.share) for t, f in mutable.items()}
        cut = {t: int(share * have[t]) for t, share in self.retire.items()}     # `retire` takes the `k`s below

        def deletes(t, ks):
            pk, f = run.tables[t].pk, facts[t]
            keys = [tuple(f.make(k)[c] for c in pk) for k in ks]
            return run.delete_ops(t, [k[0] for k in keys] if len(pk) == 1 else keys, INGEST_BATCH)

        if mutable and (self.rewrites or self.deletes):
            ops, gone = [], []
            for t, f in mutable.items():
                left = int(self.rewrites * n * f.share)
                while left > 0:                                         # a round rewrites no row twice
                    take = min(left, max(have[t] // 2, 1))
                    ops += run.ops(t, (f.make(k) for k in rng.sample(range(have[t]), take)), INGEST_BATCH)
                    left -= take
                gone += deletes(t, rng.sample(range(cut.get(t, 0), have[t]), int(self.deletes * n * f.share)))
            with run.phase("churn") as ph:
                ph.replay(ops + gone)

        if cut:
            ops = [op for t in cut for op in deletes(t, range(cut[t]))]
            with run.phase("retire") as ph:
                ph.replay(ops)

        run.crash(workers=home)
        if self.rescale != 1:
            assert run.phases[-1]["resliced"], "the boot at the run's own worker count re-sliced the log tail"


class Zipf:
    """Keys in `[1, n]` with rank `r` drawn in proportion to `1 / r**s`: a few
    hot keys take most of the rows, as a join's fan-out and an exchange's
    partitions see them in practice."""

    def __init__(self, n: int, s: float = 1.1):
        import bisect
        import itertools
        self._bisect = bisect.bisect_left
        weights = [1.0 / k ** s for k in range(1, n + 1)]
        total = sum(weights)
        self._cdf = list(itertools.accumulate(w / total for w in weights))

    def at(self, u: float) -> int:
        """The key at quantile `u` of the distribution."""
        return min(self._bisect(self._cdf, u), len(self._cdf) - 1) + 1

    def __call__(self, rng) -> int:
        return self.at(rng.random())
