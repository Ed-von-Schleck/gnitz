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
OPCODE_YIELD            = 13
OPCODE_JUMP             = 14
OPCODE_CLEAR_DELTAS     = 15
OPCODE_ANTI_JOIN_DELTA_TRACE = 16
OPCODE_ANTI_JOIN_DELTA_DELTA = 17
OPCODE_SEMI_JOIN_DELTA_TRACE = 18
OPCODE_SEMI_JOIN_DELTA_DELTA = 19

# Compile-time markers (not VM opcodes — never emitted as instructions)
OPCODE_EXCHANGE_SHARD  = 20
OPCODE_EXCHANGE_GATHER = 21

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
PARAM_JUMP_TARGET  = 4   # JUMP: target pc (kept for compatibility)
PARAM_YIELD_REASON = 5   # YIELD: reason code
PARAM_AGG_COL_IDX    = 6   # REDUCE: which column to aggregate
PARAM_EXPR_NUM_REGS  = 7   # FILTER: expression register count
PARAM_EXPR_RESULT_REG = 8  # FILTER: which register holds the result
PARAM_GATHER_WORKER   = 9   # GATHER: destination worker id
PARAM_PROJ_BASE      = 32  # MAP: slots 32..63 = source col index per output col
PARAM_EXPR_BASE      = 64  # FILTER: slots 64..255 = expression bytecode words
PARAM_SHARD_COL_BASE = 128  # SHARD: slots 128..159 = shard column indices
