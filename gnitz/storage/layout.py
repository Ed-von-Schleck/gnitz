"""
gnitz/storage/layout.py

Physical layout constants for ECS shards.
"""

MAGIC_NUMBER = 0x31305F5A54494E47
ALIGNMENT = 64
HEADER_SIZE = 64

# ECS Shard Header Offsets (64-byte header)
OFF_MAGIC = 0        # u64: Magic number
OFF_COUNT = 8        # u64: Entity count

# Checksums (DJB2 algorithm)
OFF_CHECKSUM_E = 16  # u64: Checksum of Region E (Entity IDs)
OFF_CHECKSUM_W = 24  # u64: Checksum of Region W (Weights)

# Region pointers
OFF_REG_E_ECS = 32   # u64: Offset to Region E (Entity IDs)
OFF_REG_C_ECS = 40   # u64: Offset to Region C (Component Data)
OFF_REG_B_ECS = 48   # u64: Offset to Region B (Blob Heap)
OFF_REG_W_ECS = 56   # u64: Offset to Region W (Weights)

# Legacy offsets (for backward compatibility, deprecated)
OFF_REG_W = 16
OFF_REG_O = 24
OFF_REG_K = 32
OFF_REG_V = 40
OFF_PADDING = 48

STRIDE_W = 8
STRIDE_O = 8
