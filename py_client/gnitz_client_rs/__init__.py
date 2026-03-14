"""
Thin compatibility shim: exposes the gnitz Rust extension module under
gnitz_client-compatible names.

Known API differences from gnitz_client:
  - ExprBuilder.build() returns ExprProgram object, not dict
  - CircuitBuilder.build() returns CircuitGraph object, not dataclass
  - ZSetBatch(schema) positional; keyword form also works
  - ZSetBatch.columns pre-populated with empty lists per column
  - TypeCode is a PyO3 class with class-level const attrs
  - CircuitBuilder.filter(input, expr=None) — no func_id param
"""
import gnitz

GnitzClient = gnitz.GnitzClient
GnitzError  = gnitz.GnitzError
TypeCode    = gnitz.TypeCode
ColumnDef   = gnitz.ColumnDef
Schema      = gnitz.Schema

__all__ = ["GnitzClient", "GnitzError", "TypeCode", "ColumnDef", "Schema"]
