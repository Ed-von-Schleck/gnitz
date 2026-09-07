from enum import IntEnum

from gnitz._native import circuit_opcodes, type_codes

# The column-type table comes from gnitz_wire (via `TypeCode::ALL`), so the
# names and codes cannot drift from the ones the wire, the engine and the C
# bindings use — a variant added there appears here with no edit.
TypeCode = IntEnum("TypeCode", dict(type_codes()))

# Likewise for the circuit operator space (`Opcode::ALL`). A test that asserts
# on a compiled circuit's shape names the operator; the discriminants are
# durable catalog state and belong in one place.
Opcode = IntEnum("Opcode", dict(circuit_opcodes()))
