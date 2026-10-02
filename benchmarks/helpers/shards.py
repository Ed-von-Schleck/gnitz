"""A data directory's shard files, read region by region.

`gnitz-server --disk-usage` reports each store's regions as shares; this reads
the same header and directory for the bytes themselves, so a benchmark can sum
them by class and run its own encoders over them. It follows the layout
`gnitz-zset`'s shard writer produces and refuses a file whose directory does not
span it, so a format change fails here instead of misreporting.
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from pathlib import Path

MAGIC = 0x31305F5A54494E47
HEADER_SIZE = 56
DIR_ENTRY_SIZE = 16
ALIGNMENT = 64
ENCODINGS = ["raw", "constant", "two-value", "for", "dict"]

_SLOT = re.compile(r"^(?:(.*)_)?w\d+of\d+$")


@dataclass
class Region:
    role: str          # pk, weight, null, p<N>, blob, filter
    encoding: str
    data: bytes


@dataclass
class Shard:
    path: Path
    relation: int
    store: str         # "rows", "scratch_<child>", "idx_<cols>", "delta"
    rows: int
    size: int
    regions: list[Region]

    @property
    def overhead(self):
        """Header, directory and alignment padding."""
        return self.size - sum(len(r.data) for r in self.regions)

    @property
    def retractions(self):
        """Rows of negative weight: each cancels a row some other shard of the store holds."""
        w = next(r for r in self.regions if r.role == "weight")
        if w.encoding == "constant":
            return self.rows if struct.unpack("<q", w.data)[0] < 0 else 0
        if w.encoding == "two-value":
            a, b = struct.unpack_from("<qq", w.data)
            ones = int.from_bytes(w.data[16:], "little").bit_count()
            return (ones if b < 0 else 0) + (self.rows - ones if a < 0 else 0)
        if w.encoding == "for":
            # The reference is the least weight, and every offset is non-negative.
            (ref,) = struct.unpack_from("<q", w.data)
            if ref >= 0:
                return 0
            bw = (len(w.data) - 15) // self.rows
            cells = w.data[8:8 + self.rows * bw]
            return sum(1 for i in range(self.rows) if int.from_bytes(cells[i * bw:(i + 1) * bw], "little") < -ref)
        return sum(1 for (v,) in struct.iter_unpack("<q", w.data) if v < 0)


def read_shard(path, relation=0, store="rows"):
    image = Path(path).read_bytes()
    magic, _version, rows, _desc, npc, _flags, _body = struct.unpack_from("<7Q", image, 0)
    if magic != MAGIC:
        raise ValueError(f"{path}: not a shard")
    roles = ["pk", "weight", "null"] + [f"p{i}" for i in range(npc)] + ["blob", "filter"]
    end = HEADER_SIZE + len(roles) * DIR_ENTRY_SIZE
    regions = []
    for i, role in enumerate(roles):
        size, encoding = struct.unpack_from("<QB", image, HEADER_SIZE + i * DIR_ENTRY_SIZE)
        off = -(-end // ALIGNMENT) * ALIGNMENT
        end = off + size
        regions.append(Region(role, ENCODINGS[encoding], image[off:end]))
    if end != len(image):
        raise ValueError(f"{path}: directory does not span the file")
    return Shard(Path(path), relation, store, rows, len(image), regions)


def read_shards(data_dir, min_relation=0):
    """Every shard under `data_dir` of a relation id at or past `min_relation`."""
    out = []
    for rel_dir in sorted((Path(data_dir) / "_relations").iterdir()):
        if not rel_dir.name.isdigit() or int(rel_dir.name) < min_relation:
            continue
        for path in sorted(rel_dir.rglob("shard_*.db")):
            child = path.parent.name if path.parent != rel_dir else ""
            m = _SLOT.match(child)
            store = (m.group(1) if m else None) or "rows"
            out.append(read_shard(path, int(rel_dir.name), store))
    return out


def region_class(region):
    """The class a region's bytes are summed under."""
    if region.role[0] == "p" and region.role[1:].isdigit():
        return f"payload {region.encoding}"
    return region.role
