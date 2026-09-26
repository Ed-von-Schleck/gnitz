"""Schemas that more than one test module creates a table from through the
binary API."""

import gnitz

# `(pk U64, val I64)`, keyed on `pk`.
KV = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                   gnitz.ColumnDef("val", gnitz.TypeCode.I64)], [0])
