"""What a kept `bench-disk` data directory would weigh under other encodings.

Reads the directories `disk.py --keep` left behind and re-encodes each shard
region's real bytes under candidate encodings, so an idea is priced on the bytes
it would act on. Every candidate is sized as the image its region would hold;
alignment padding is not re-derived. Region types are inferred from the bytes:
a 16-byte raw payload column whose every cell reads as a German string against
the shard's heap is a string column.

    uv run --with numpy python ../../benchmarks/disk_whatif.py ../../tmp/bench_disk_*
"""

from __future__ import annotations

import compression.zstd as zstd
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "benchmarks"))
from helpers.shards import read_shards  # noqa: E402

FIRST_USER = 16
BLOCK = 4096          # bytes per independently compressed heap block


def bits_for(n):
    """Bits to tell `n` values apart."""
    return max(int(n - 1).bit_length(), 0)


def packed(rows, bits):
    return (rows * bits + 7) // 8


# -- integers ---------------------------------------------------------------

def ints_of(data, rows):
    """A raw fixed-width region as unsigned little-endian integers (≤ 8 bytes)."""
    w = len(data) // rows
    return np.frombuffer(data, dtype=f"<u{w}").astype(np.uint64), w


def for_decode(data, rows):
    bw = (len(data) - 15) // rows
    ref = int.from_bytes(data[:8], "little")
    cells = np.frombuffer(data[8:8 + rows * bw], dtype=np.uint8).reshape(rows, bw)
    v = np.zeros(rows, dtype=np.uint64)
    for b in range(bw):
        v |= cells[:, b].astype(np.uint64) << np.uint64(8 * b)
    return v + np.uint64(ref), bw


def int_candidates(v, w, rows):
    """Sizes of an integer column's images: byte FoR, bit FoR, dictionary, delta."""
    span = int(v.max() - v.min())
    bw = max((span.bit_length() + 7) // 8, 1)
    out = {"for_bytes": min(8 + rows * bw + 7, rows * w), "for_bits": 9 + packed(rows, span.bit_length())}
    distinct = len(np.unique(v))
    out["dict"] = distinct * w + packed(rows, bits_for(distinct)) + 8
    # Block-wise bit-packed deltas, one width and one base per 128 rows.
    d = np.diff(v.astype(np.int64))
    zz = ((d << 1) ^ (d >> 63)).astype(np.uint64)
    total = 0
    for i in range(0, len(zz), 128):
        total += 9 + packed(len(zz[i:i + 128]), int(zz[i:i + 128].max()).bit_length())
    out["delta_bits"] = total + w
    return out


# -- keys -------------------------------------------------------------------

def pk_candidates(data, rows):
    w = len(data) // rows
    cells = np.frombuffer(data, dtype=np.uint8).reshape(rows, w)
    varying = int((cells != cells[0]).any(axis=0).sum())
    out = {"elide_const_bytes": rows * varying + 2 * w}
    # Leading bytes every key of the shard shares.
    lead = 0
    while lead < w and not (cells[:, lead] != cells[0, lead]).any():
        lead += 1
    out["strip_common_prefix"] = rows * (w - lead) + w
    # Bits that vary anywhere in the shard, packed.
    diff = np.bitwise_or.reduce(cells ^ cells[0], axis=0)
    varying_bits = sum(int(b).bit_count() for b in diff)
    out["elide_const_bits"] = packed(rows, varying_bits) + 2 * w
    # Sorted keys as integers: block-wise bit-packed deltas with one full key per block.
    if w <= 8:
        pad = np.zeros((rows, 8), dtype=np.uint8)
        pad[:, 8 - w:] = cells
        d = np.diff(pad.view(">u8").ravel().astype(np.uint64))
        widths = [int(d[i:i + 127].max()).bit_length() for i in range(0, len(d), 128)]
    else:
        ints = [int.from_bytes(bytes(c), "big") for c in cells]
        widths = [max((b - a for a, b in zip(ints[i:i + 127], ints[i + 1:i + 128])), default=0).bit_length()
                  for i in range(0, rows, 128)]
    out["delta_bits_128"] = sum(w + 1 + packed(127, b) for b in widths)
    return out


# -- strings ----------------------------------------------------------------

def string_cells(data, rows, heap):
    """Each cell's content, or None unless every cell reads as a German string."""
    if len(data) != rows * 16:
        return None
    out = []
    for i in range(rows):
        cell = data[16 * i:16 * i + 16]
        n = int.from_bytes(cell[:4], "little")
        if n <= 12:
            if any(cell[4 + n:]):
                return None
            out.append(cell[4:4 + n])
        else:
            off = int.from_bytes(cell[8:], "little")
            if off + n > len(heap) or heap[off:off + 4] != cell[4:8]:
                return None
            out.append(heap[off:off + n])
    return out


def dict_cells(data, rows, heap):
    n = int.from_bytes(data[:8], "little")
    entries = string_cells(data[8:8 + 16 * n], n, heap)
    return entries, n


def block_zstd(blob, level=3, dictionary=None):
    """`blob` compressed in independent `BLOCK`-byte blocks, with a 4-byte offset per block."""
    kw = {"zstd_dict": dictionary} if dictionary else {}
    return sum(len(zstd.compress(blob[i:i + BLOCK], level=level, **kw)) + 4 for i in range(0, len(blob), BLOCK))


# -- one shard --------------------------------------------------------------

def price_shard(shard, acc):
    """Add `shard`'s current bytes and each idea's saving to `acc`."""
    rows = shard.rows
    regions = {r.role: r for r in shard.regions}
    heap = regions["blob"].data
    acc["current"] += shard.size
    acc["overhead (header, directory, padding)"] += shard.overhead
    acc["filter"] += len(regions["filter"].data)
    heap_from_raw = 0                     # heap bytes named by raw (undictionaried) cells
    long_contents = []

    for r in shard.regions:
        n = len(r.data)
        if r.role == "pk":
            if r.encoding == "raw" and rows > 1:
                for name, size in pk_candidates(r.data, rows).items():
                    acc[f"pk: {name}"] += n - min(size, n)
        elif r.role == "weight":
            if r.encoding == "raw":
                v, w = ints_of(r.data, rows)
                c = int_candidates(v, w, rows)
                acc["weight: byte FoR"] += n - min(c["for_bytes"], n)
                acc["weight: best of bit FoR / dict"] += n - min(c["for_bits"], c["dict"], n)
        elif r.role == "null":
            if r.encoding == "raw":
                v, _ = ints_of(r.data, rows)
                npc = len(shard.regions) - 5
                acc["null: one byte per 8 columns"] += n - rows * ((npc + 7) // 8)
                acc["null: dictionary of bitmap words"] += n - min(
                    n, 8 * len(np.unique(v)) + packed(rows, bits_for(len(np.unique(v)))))
                cols = int(np.bitwise_or.reduce(v)).bit_count()
                acc["null: one bit per nullable column"] += n - packed(rows, cols)
        elif r.role in ("blob", "filter"):
            pass
        elif r.encoding == "dict":
            entries, d = dict_cells(r.data, rows, heap)
            code = 1 if d <= 256 else 2
            acc["string dict: bit-packed codes"] += rows * code - packed(rows, bits_for(d))
            acc["string dict: entries as length + bytes"] += sum(16 - (1 + (len(e) if len(e) <= 12 else 0))
                                                               for e in entries)
            long_contents += [e for e in entries if len(e) > 12]
        elif r.encoding in ("raw", "for"):
            cells = string_cells(r.data, rows, heap) if r.encoding == "raw" else None
            if cells is not None:
                distinct = len(set(cells))
                dict_size = 8 + 16 * distinct + rows * (1 if distinct <= 256 else 2)
                if distinct <= 65536 and dict_size < n:
                    acc["string raw: dictionary the writer skipped"] += n - dict_size
                # A length per row in place of the 16-byte cell; inline content moves to the heap.
                lens = np.array([len(c) for c in cells])
                inline = int(lens[lens <= 12].sum())
                lw = 1 if lens.max() < 256 else 2
                acc["string raw: length + heap in place of cell"] += n - (rows * lw + inline)
                heap_from_raw += int(lens[lens > 12].sum())
                long_contents += [c for c in cells if len(c) > 12]
            elif (w := (n // rows if r.encoding == "raw" else 0)) in (2, 4, 8) or r.encoding == "for":
                if r.encoding == "raw":
                    v, w = ints_of(r.data, rows)
                else:
                    v, w = for_decode(r.data, rows)
                    w = 8
                c = int_candidates(v, w, rows)
                if r.encoding == "raw":
                    acc["int raw: byte FoR"] += n - min(c["for_bytes"], n)
                acc["int: bit FoR"] += n - min(c["for_bits"], n)
                acc["int: dictionary"] += n - min(c["dict"], n)
                acc["int: delta bits"] += n - min(c["delta_bits"], n)
                acc["int: best of all"] += n - min(min(c.values()), n)
            else:
                acc["16-byte raw non-string (uuid/u128)"] += 0
                v = np.frombuffer(r.data, dtype=np.dtype([("a", "<u8"), ("b", "<u8")]))
                distinct = len(np.unique(v))
                acc["wide int: dictionary"] += n - min(n, 16 * distinct + packed(rows, bits_for(distinct)))

    if heap:
        acc["heap bytes"] += len(heap)
        whole = len(zstd.compress(heap, level=3))
        acc["heap: zstd-3 whole"] += len(heap) - whole
        acc[f"heap: zstd-3 in {BLOCK}-byte blocks"] += len(heap) - min(block_zstd(heap), len(heap))
        sample = long_contents[:: max(len(long_contents) // 2000, 1)]
        if len(sample) >= 16:
            try:
                d = zstd.train_dict(sample, 16 << 10)
                per_value = sum(len(zstd.compress(c, level=3, zstd_dict=d)) for c in long_contents[::7]) * 7
                acc["heap: zstd-3 per value, 16 KiB trained dict"] += len(heap) - min(per_value + len(d.dict_content), len(heap))
            except zstd.ZstdError:
                pass


def main():
    dirs = [Path(p) for p in sys.argv[1:]]
    table = {}
    for d in sorted(dirs):
        rec = json.loads((d / "result.json").read_text())
        key = f"{rec['scenario']}/{rec['regime']}"
        acc = defaultdict(int)
        for shard in read_shards(d / "data", FIRST_USER):
            price_shard(shard, acc)
        live = sum(rec["relations"].values())
        on_disk = {(s["relation"], s["store"]): s for s in rec["stores"]}
        # Rows a store holds past its relation's live rows: retractions and the rows they cancel.
        for (rel, store), s in on_disk.items():
            if store == "rows":
                extra = s["rows"] - rec["relations"][s["name"]]
                if extra > 0:
                    acc["uncancelled rows in output stores (bytes pro rata)"] += s["bytes"] * extra // s["rows"]
        acc["identical second copies (reduce output = its trace)"] = rec["identical_bytes"]
        table[key] = acc
        print(f"\n== {key}: {acc['current']} shard bytes, {live} live rows")
        for name, saved in sorted(acc.items(), key=lambda kv: -kv[1]):
            if name != "current" and saved:
                print(f"  {saved:>12} {100 * saved / acc['current']:>5.1f}%  {name}")
    out = REPO_ROOT / "benchmarks/results/disk/whatif.json"
    out.write_text(json.dumps(table, indent=1))
    print(f"\nresults: {out}")


if __name__ == "__main__":
    main()
