MAGIC_NUMBER = 0x31305F5A54494E47
ALIGNMENT = 64
HEADER_SIZE = 64

# --- Legacy Key-Value Shard Header Offsets ---
OFF_MAGIC       = 0
OFF_COUNT       = 8
OFF_REG_W       = 16 # Legacy Weight Region (Key-Value)
OFF_REG_O       = 24 # Legacy Offset Region (Key-Value)
OFF_REG_K       = 32 # Legacy Key Heap (Key-Value)
OFF_REG_V       = 40 # Legacy Value Heap (Key-Value)
OFF_PADDING     = 48

# --- Legacy Key-Value Shard Strides ---
STRIDE_W = 8 
STRIDE_O = 8

# --- New ECS Shard Header Offsets ---
# OFF_MAGIC (0) and OFF_COUNT (8) are shared.
# Bytes 16-31 are reserved for LSNs in a future step.
OFF_REG_E_ECS   = 32 # Region E: Entity IDs (u64)
OFF_REG_C_ECS   = 40 # Region C: Component Data (fixed-stride)
OFF_REG_B_ECS   = 48 # Region B: Blob Heap (variable-length)```
