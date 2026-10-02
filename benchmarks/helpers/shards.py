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
HEADER_SIZE = 64
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
    retractions: int   # rows of negative weight: each cancels a row some other shard of the store holds
    size: int
    regions: list[Region]

    @property
    def overhead(self):
        """Header, directory and alignment padding."""
        return self.size - sum(len(r.data) for r in self.regions)


def read_shard(path, relation=0, store="rows"):
    image = Path(path).read_bytes()
    magic, _version, rows, _desc, npc, _flags, _body, retractions = struct.unpack_from("<8Q", image, 0)
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
    return Shard(Path(path), relation, store, rows, retractions, len(image), regions)


def store_of(path):
    """The `(relation, store)` whose shard `path` names, or None for any other file."""
    path = Path(path)
    parts = path.parts
    if "_relations" not in parts or not re.fullmatch(r"shard_\d+\.db", path.name):
        return None
    relation, *children = parts[parts.index("_relations") + 1:-1]
    m = _SLOT.match(children[-1]) if children else None
    return int(relation), (m.group(1) if m else None) or "rows"


def read_shards(data_dir, min_relation=0):
    """Every shard under `data_dir` of a relation id at or past `min_relation`."""
    out = []
    for path in sorted((Path(data_dir) / "_relations").rglob("shard_*.db")):
        relation, store = store_of(path)
        if relation >= min_relation:
            out.append(read_shard(path, relation, store))
    return out


def region_class(region):
    """The class a region's bytes are summed under."""
    if region.role[0] == "p" and region.role[1:].isdigit():
        return f"payload {region.encoding}"
    return region.role
