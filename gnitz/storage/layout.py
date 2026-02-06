"""
gnitz/storage/layout.py

Physical layout constants for ECS shard files.
"""

# File format identification
MAGIC_NUMBER = 0x31305F5A54494E47  # "GNITZ_01" in ASCII

# Alignment and sizing
ALIGNMENT = 64
HEADER_SIZE = 64

# Header field offsets (all fields are 8 bytes / u64)
OFF_MAGIC       = 0   # Magic number for format validation
OFF_COUNT       = 8   # Number of entities in shard
OFF_CHECKSUM_E  = 16  # Checksum for Region E (Entity IDs)
OFF_CHECKSUM_W  = 24  # Checksum for Region W (Weights)

# Region pointer offsets (point to start of each region)
OFF_REG_E_ECS   = 32  # Region E: Entity IDs (u64 array)
OFF_REG_C_ECS   = 40  # Region C: Component Data (fixed-stride array)
OFF_REG_B_ECS   = 48  # Region B: Blob Heap (variable-length data)
OFF_REG_W_ECS   = 56  # Region W: Weights (i64 array)
