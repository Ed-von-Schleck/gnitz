"""
gnitz/storage/layout.py

Physical layout constants for ECS shards.
"""
from rpython.rlib.rarithmetic import r_uint64

MAGIC_NUMBER = r_uint64(0x31305F5A54494E47)
ALIGNMENT = 64
HEADER_SIZE = 64

OFF_MAGIC = 0
OFF_COUNT = 8

OFF_CHECKSUM_E = 16
OFF_CHECKSUM_W = 24

OFF_REG_E_ECS = 32
OFF_REG_C_ECS = 40
OFF_REG_B_ECS = 48
OFF_REG_W_ECS = 56
