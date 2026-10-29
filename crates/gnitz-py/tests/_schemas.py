"""Schemas that more than one test module creates a table from through the
binary API."""

import gnitz

# `(pk U64, val I64)`, keyed on `pk`.
KV = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                   gnitz.ColumnDef("val", gnitz.TypeCode.I64)], [0])


def kv(*rows):
    """A `KV` batch of `(pk, val)` rows at weight 1, or `(pk, val, weight)` ones."""
    return gnitz.ZSetBatch(KV).extend(dict(zip(("pk", "val", "_weight"), r)) for r in rows)


# A relation every connection can read.
SCHEMAS = "_system.schemas"
