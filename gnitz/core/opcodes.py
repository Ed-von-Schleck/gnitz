# gnitz/core/opcodes.py
#
# Shared constants for circuit graph construction and VM execution.
# This module must import nothing from gnitz. It is safe to import on
# the client side.

# --- Opcodes (match Instruction.* constants in instructions.py) ---
OPCODE_HALT             = 0
OPCODE_FILTER           = 1
OPCODE_MAP              = 2
OPCODE_NEGATE           = 3
OPCODE_UNION            = 4
OPCODE_JOIN_DELTA_TRACE = 5
OPCODE_JOIN_DELTA_DELTA = 6
OPCODE_INTEGRATE        = 7
OPCODE_DELAY            = 8
OPCODE_REDUCE           = 9
OPCODE_DISTINCT         = 10
OPCODE_SCAN_TRACE       = 11
OPCODE_SEEK_TRACE       = 12
OPCODE_CLEAR_DELTAS     = 15
OPCODE_ANTI_JOIN_DELTA_TRACE = 16
OPCODE_ANTI_JOIN_DELTA_DELTA = 17
OPCODE_SEMI_JOIN_DELTA_TRACE = 18
OPCODE_SEMI_JOIN_DELTA_DELTA = 19
OPCODE_JOIN_DELTA_TRACE_OUTER = 22
# 23 reserved for OPCODE_JOIN_DELTA_DELTA_OUTER (future)

# Compile-time markers (not VM opcodes — never emitted as instructions)
OPCODE_EXCHANGE_SHARD  = 20

# --- Input port indices (dst_port values in _circuit_edges) ---
# Unary ops
PORT_IN       = 0
# Binary ops
PORT_IN_A     = 0
PORT_IN_B     = 1
# Join
PORT_DELTA    = 0
PORT_TRACE    = 1
# Reduce
PORT_IN_REDUCE     = 0
PORT_TRACE_IN      = 1
PORT_TRACE_OUT     = 2
# Distinct
PORT_IN_DISTINCT   = 0
PORT_HISTORY       = 1
# Exchange
PORT_EXCHANGE_IN   = 0

# --- Parameter slot indices (slot values in _circuit_params) ---
PARAM_FUNC_ID      = 0   # FILTER, MAP: scalar function id
PARAM_AGG_FUNC_ID  = 1   # REDUCE: aggregate function id
PARAM_CHUNK_LIMIT  = 2   # SCAN_TRACE: max records per tick
PARAM_TABLE_ID     = 3   # INTEGRATE: target table id
PARAM_AGG_COL_IDX    = 6   # REDUCE: which column to aggregate
PARAM_EXPR_NUM_REGS  = 7   # FILTER: expression register count
PARAM_EXPR_RESULT_REG = 8  # FILTER: which register holds the result
PARAM_REINDEX_COL        = 10   # MAP: use column value as new PK
PARAM_JOIN_SOURCE_TABLE  = 11   # SCAN_TRACE: source table for multi-input circuits
PARAM_AGG_COUNT          = 12   # REDUCE: number of aggregate functions
PARAM_AGG_SPEC_BASE      = 13   # REDUCE: packed (func_id << 32 | col_idx) per agg
PARAM_PROJ_BASE      = 32  # MAP: slots 32..63 = source col index per output col
PARAM_EXPR_BASE      = 64  # FILTER, MAP: slots 64..255 = expression bytecode words
PARAM_SHARD_COL_BASE = 128  # SHARD: slots 128..159 = shard column indices
PARAM_CONST_STR_BASE = 160  # Slots 160-191: string constants for expressions
