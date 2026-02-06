"""
gnitz/storage/layout.py
"""
MAGIC_NUMBER = 0x31305F5A54494E47
ALIGNMENT = 64
HEADER_SIZE = 64

# --- New ECS Shard Header Offsets ---
# Bytes 0-31 are mostly shared/reserved
OFF_MAGIC       = 0
OFF_COUNT       = 8
# OFF_RESERVED (16-31)

# Region Offsets
OFF_REG_E_ECS   = 32 # Region E: Entity IDs (u64)
OFF_REG_C_ECS   = 40 # Region C: Component Data (fixed-stride)
OFF_REG_B_ECS   = 48 # Region B: Blob Heap (variable-length)
OFF_REG_W_ECS   = 56 # Region W: Weights (i64) - NEW

# Legacy Key-Value offsets (retained for compatibility if needed, else ignored)
OFF_REG_W       = 16 
OFF_REG_O       = 24
OFF_REG_K       = 32
OFF_REG_V       = 40
OFF_PADDING     = 48
