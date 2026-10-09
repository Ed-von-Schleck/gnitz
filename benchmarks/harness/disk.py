"""A stopped server's data directory, by relation, store, LSM level and region.

Taken after a graceful stop, whose checkpoint puts every store a boot resumes
on disk: `gnitz-server --disk-usage` for the bytes by relation, store and LSM
level, and the same shards summed by region class — key, weight, null bitmap,
payload by encoding, string heap, PK filter, and the header, directory and
alignment padding around them.

No checkpoint publishes a view a stream reaches; what such a store spilled
counts as written, and as no part of the footprint.

"Value bytes" is what the live rows hold: each number at its column's width,
each string at its length, a NULL at nothing. "Pushed" is the same measure over
every row a scenario sent, the rewritten and the deleted ones included.
"""

from __future__ import annotations

import compression.zstd as zstd
import re
from collections import defaultdict

import gnitz
from _serverproc import disk_usage

from .shards import read_shards, region_class

FS_BLOCK = 4096
MAX_STORE_LINES = 40


def zstd_bytes(blobs):
    """What a general-purpose compressor leaves of `blobs`, each on its own."""
    return sum(len(zstd.compress(b, level=3)) for b in blobs if b)


def snapshot(data_dir, relations, tables, value_bytes, pushed_bytes):
    """The footprint record of `data_dir`. `relations` is `{id: (name, live rows)}`,
    `tables` the ids among them that are base tables."""
    report, stores = disk_usage(data_dir)
    system = {s["line"] for s in stores if s["relation"] < gnitz.FIRST_USER_TABLE_ID}
    # Stores no checkpoint published.
    unnamed = {(s["relation"], s["store"]) for s in stores if s["level"] == "-"}
    assert not unnamed & {(s["relation"], s["store"]) for s in stores if s["level"] != "-"}, \
        "a clean shutdown leaves no published store a shard its manifest does not name"
    on_disk = read_shards(data_dir, gnitz.FIRST_USER_TABLE_ID)
    assert sum(s.size for s in on_disk) == sum(s["bytes"] for s in stores if s["line"] not in system), \
        "the shards read here are the ones the server reports"
    shards = [s for s in on_disk if (s.relation, s.store) not in unnamed]
    shard_bytes = sum(s.size for s in shards)

    # (relation, store) -> class -> bytes, and what zstd leaves of each store.
    by_store = defaultdict(lambda: defaultdict(int))
    blobs = defaultdict(list)
    for s in shards:
        key = (s.relation, s.store)
        by_store[key]["overhead"] += s.overhead
        by_store[key]["rows"] += s.rows
        by_store[key]["retractions"] += s.retractions
        by_store[key]["files"] += 1
        by_store[key]["blocks"] += -(-s.size // FS_BLOCK) * FS_BLOCK
        for r in s.regions:
            by_store[key][region_class(r)] += len(r.data)
            blobs[key].append(r.data)
    store_records = []
    for (rel, store), classes in sorted(by_store.items()):
        rows, retractions = classes.pop("rows", 0), classes.pop("retractions", 0)
        files, blocks = classes.pop("files", 0), classes.pop("blocks", 0)
        store_records.append({
            "relation": rel, "name": relations.get(rel, (f"relation {rel}", 0))[0], "table": rel in tables,
            "store": store, "rows": rows,
            "retractions": retractions, "files": files, "block_bytes": blocks,
            "bytes": sum(classes.values()), "zstd3": zstd_bytes(blobs[(rel, store)]),
            "classes": dict(sorted(classes.items())),
        })

    identical = sum(int(line.split()[4]) for line in report.splitlines() if line.startswith("identical shards:"))
    return {
        "value_bytes": value_bytes, "pushed_bytes": pushed_bytes,
        "shard_bytes": shard_bytes, "unnamed_bytes": sum(s.size for s in on_disk) - shard_bytes,
        "identical_bytes": identical,
        "relations": {name: live for name, live in relations.values()},
        "ids": {str(rel): name for rel, (name, _) in relations.items()},
        "stores": store_records,
        "report": "\n".join(line for line in report.splitlines() if line not in system),
    }


def totals(snap):
    """One snapshot as the summary's columns."""
    # A relation that is no base table is a view, or one the planner put under a view.
    relations = [s for s in snap["stores"] if s["store"] == "rows"]
    views = sum(s["bytes"] for s in relations if not s["table"])
    rows = sum(s["rows"] for s in snap["stores"])
    return {
        "shard_bytes": snap["shard_bytes"],
        "per_value_byte": snap["shard_bytes"] / snap["value_bytes"] if snap["value_bytes"] else None,
        "files": sum(s["files"] for s in snap["stores"]),
        "block_bytes": sum(s["block_bytes"] for s in snap["stores"]),
        "base_bytes": sum(s["bytes"] for s in relations) - views,
        "view_bytes": views,
        "trace_index_bytes": sum(s["bytes"] for s in snap["stores"] if s["store"] != "rows"),
        "zstd3_ratio": sum(s["zstd3"] for s in snap["stores"]) / max(snap["shard_bytes"], 1),
        # A retraction and the row it cancels are both dead.
        "dead_rows": 2 * sum(s["retractions"] for s in snap["stores"]) / max(rows, 1),
    }


def written(writes):
    """The `(written, published)` shard bytes of a reading of the `writes`
    layer, or `(None, None)` for a run it did not trace."""
    if writes is None:
        return None, None
    return sum(w for w, _ in writes.values()), sum(p for _, p in writes.values())


def print_snapshot(title, snap, writes=None):
    """`writes` is the `writes` layer's `{"<relation> <store>": [written, published]}`
    up to this reading, or None."""
    print(f"\n--- disk, {title}" + (f": {snap['value_bytes']} value bytes live, {snap['pushed_bytes']} pushed"
                                      if snap["value_bytes"] else ""))
    for name, live in list(snap["relations"].items())[:12]:
        print(f"  {name}: {live} rows")
    traced = writes is not None
    only_wrote, stores = dict(writes or {}), []
    for s in snap["stores"]:
        wrote, published = only_wrote.pop(f"{s['relation']} {s['store']}", (0, 0)) if traced else (None, None)
        stores.append(dict(s, written=wrote, published=published))
    # A line for a store that only wrote.
    for key, (wrote, published) in only_wrote.items():
        rel, store = key.split(" ", 1)
        stores.append(dict(relation=int(rel), name=snap["ids"].get(rel, f"relation {rel}"), store=store, rows=0,
                           retractions=0, files=0, bytes=0, zstd3=0, classes={}, written=wrote, published=published))
    stores.sort(key=lambda s: (s["relation"], s["store"]))
    report = snap["report"].splitlines()
    if len(stores) > MAX_STORE_LINES:
        # One line per store would bury the totals: sum the stores whose names differ in digits alone.
        report = [line for line in report if not line.split(maxsplit=1)[0].isdigit()]
        merged = {}
        for s in stores:
            m = merged.setdefault((re.sub(r"\d+", "*", s["name"]), s["store"]), dict(
                s, name=re.sub(r"\d+", "*", s["name"]), rows=0, retractions=0, files=0, bytes=0, zstd3=0,
                written=0 if traced else None, published=0 if traced else None, classes=defaultdict(int)))
            for k in ("rows", "retractions", "files", "bytes", "zstd3") + (("written", "published") if traced else ()):
                m[k] += s[k]
            for c, n in s["classes"].items():
                m["classes"][c] += n
        stores = list(merged.values())
    print("\n".join(report))
    classes = sorted({c for s in stores for c in s["classes"]})
    # `retract` is the rows of negative weight; `live` the rows a scan of the relation
    # returns; `written` every shard byte the store wrote to hold `bytes`, and `published`
    # the part of it a manifest named, which is what the store synced.
    print(f"{'store':<28} {'files':>5} {'rows':>9} {'retract':>8} {'live':>8} {'bytes':>11} {'B/row':>6} "
          f"{'zstd-3':>6} {'written':>11} {'published':>11}  " + " ".join(f"{c:>16}" for c in classes))
    for s in stores:
        label = f"{s['name']} {s['store']}"
        live = snap["relations"].get(s["name"], "") if s["store"] == "rows" else ""
        cells = " ".join(f"{s['classes'].get(c, 0):>16}" for c in classes)
        print(f"{label:<28} {s['files']:>5} {s['rows']:>9} {s['retractions']:>8} {live:>8} {s['bytes']:>11} "
              f"{s['bytes'] / max(s['rows'], 1):>6.1f} {s['zstd3'] / max(s['bytes'], 1):>6.2f} "
              f"{'' if s['written'] is None else s['written']:>11} "
              f"{'' if s['published'] is None else s['published']:>11}  {cells}")
    wrote, published = written(writes)
    print((f"shard bytes per value byte: {snap['shard_bytes'] / snap['value_bytes']:.2f}" if snap["value_bytes"]
           else f"shard bytes: {snap['shard_bytes']}")
          + ("" if wrote is None else
             f"; the workers wrote {wrote} shard bytes and published {published}"
             + (f", {wrote / snap['pushed_bytes']:.2f} and "
                f"{published / snap['pushed_bytes']:.2f} per value byte pushed" if snap["value_bytes"] else ""))
          + (f"; {snap['identical_bytes']} bytes are a second copy of an identical shard"
             if snap["identical_bytes"] else "")
          + (f"; {snap['unnamed_bytes']} more bytes are spill no manifest names" if snap["unnamed_bytes"] else ""))
