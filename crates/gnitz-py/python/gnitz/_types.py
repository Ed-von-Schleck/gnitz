from enum import IntEnum

from gnitz._native import type_codes

# The column-type table comes from gnitz_wire (via `TypeCode::ALL`), so the
# names and codes cannot drift from the ones the wire, the engine and the C
# bindings use — a variant added there appears here with no edit.
TypeCode = IntEnum("TypeCode", dict(type_codes()))
