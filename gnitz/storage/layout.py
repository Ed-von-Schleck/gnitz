"""
Physical Layout Definitions for Quad-Partition Shards.
Structure (64-byte Header):
  [00-07] Magic Number (GNITZ_01)
  [08-15] Entry Count (N)
  [16-23] Offset to Region W (Weights)
  [24-31] Offset to Region O (Offsets)
  [32-39] Offset to Region K (Key Heap)
  [40-47] Offset to Region V (Value Heap)
  [48-63] Padding (Reserved)
"""

# Magic Number: "GNITZ_01" in ASCII
# 0x31305F5A54494E47 (Little Endian)
MAGIC_NUMBER = 0x31305F5A54494E47

# Alignment Requirement (Cache Line Size)
# All regions must start at an offset divisible by this.
ALIGNMENT = 64

# Fixed Header Size
HEADER_SIZE = 64

# Byte Offsets within the Header
OFF_MAGIC       = 0
OFF_COUNT       = 8
OFF_REG_W       = 16
OFF_REG_O       = 24
OFF_REG_K       = 32
OFF_REG_V       = 40
OFF_PADDING     = 48

# Stride sizes for Fixed-Width Regions
# Region W: int64_t weights
STRIDE_W = 8 
# Region O: int32_t key_offset + int32_t val_offset
STRIDE_O = 8
