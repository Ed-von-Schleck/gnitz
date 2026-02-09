"""
gnitz/storage/layout.py

Physical layout constants for ECS shards.
"""
from rpython.rlib.rarithmetic import r_uint64

MAGIC_NUMBER = r_uint64(0x31305F5A54494E47)
ALIGNMENT = 64
HEADER_SIZE = 64
FOOTER_SIZE = 16  # 8 bytes for Checksum C, 8 bytes for Checksum B

OFF_MAGIC = 0
OFF_COUNT = 8

OFF_CHECKSUM_E = 16
OFF_CHECKSUM_W = 24

OFF_REG_E_ECS = 32
OFF_REG_C_ECS = 40
OFF_REG_B_ECS = 48
OFF_REG_W_ECS = 56

# Footer offsets relative to the end of the file
OFF_FOOTER_CHECKSUM_C = 16
OFF_FOOTER_CHECKSUM_B = 8
