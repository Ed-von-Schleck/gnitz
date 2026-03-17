use crate::error::ProtocolError;

pub const MAGIC_V2:    u64   = 0x474E49545A325043;
pub const HEADER_SIZE: usize = 96;
pub const ALIGNMENT:   usize = 64;

pub const FLAG_ALLOCATE_TABLE_ID:  u64 = 1;
pub const FLAG_ALLOCATE_SCHEMA_ID: u64 = 2;
pub const FLAG_SHUTDOWN:           u64 = 4;
pub const FLAG_DDL_SYNC:           u64 = 8;
pub const FLAG_EXCHANGE:           u64 = 16;
pub const FLAG_PUSH:               u64 = 32;
pub const FLAG_HAS_PK:             u64 = 64;
pub const FLAG_SEEK:               u64 = 128;
pub const FLAG_SEEK_BY_INDEX:      u64 = 256;
pub const FLAG_ALLOCATE_INDEX_ID:  u64 = 512;

pub const STATUS_OK:    u32 = 0;
pub const STATUS_ERROR: u32 = 1;

pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK:    u64 = 2;

pub const IPC_STRING_STRIDE:      usize = 8;
pub const IPC_NULL_STRING_OFFSET: u32   = 0xFFFF_FFFF;

/// Wire layout (LE, 96 bytes):
///   magic[8] status[4] err_len[4] target_id[8] client_id[8]
///   schema_count[8] schema_blob_sz[8] data_count[8] data_blob_sz[8]
///   [64] p4[8]               [72] flags[8]
///   [80] p5 seek_pk_lo[8]     [88] p6 seek_pk_hi[8]
///
/// p4/p5/p6 usage by flag:
///   FLAG_SEEK           — p5=pk_lo, p6=pk_hi, p4=0
///   FLAG_SEEK_BY_INDEX  — p4=col_idx, p5=index_key_lo, p6=index_key_hi
///   (all other flags)   — p4=p5=p6=0
pub struct Header {
    pub magic:          u64,
    pub status:         u32,
    pub err_len:        u32,
    pub target_id:      u64,
    pub client_id:      u64,
    pub schema_count:   u64,
    pub schema_blob_sz: u64,
    pub data_count:     u64,
    pub data_blob_sz:   u64,
    pub p4:  u64,
    pub flags:          u64,
    pub seek_pk_lo:     u64,   // bytes 80-87; 0 for non-seek messages
    pub seek_pk_hi:     u64,   // bytes 88-95; 0 for non-seek messages
}

impl Header {
    #[allow(unused_assignments)]
    pub fn pack(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        let mut off = 0;

        macro_rules! write_le {
            ($val:expr, $size:expr) => {{
                let bytes = $val.to_le_bytes();
                buf[off..off + $size].copy_from_slice(&bytes);
                off += $size;
            }};
        }

        write_le!(self.magic,          8);
        write_le!(self.status,         4);
        write_le!(self.err_len,        4);
        write_le!(self.target_id,      8);
        write_le!(self.client_id,      8);
        write_le!(self.schema_count,   8);
        write_le!(self.schema_blob_sz, 8);
        write_le!(self.data_count,     8);
        write_le!(self.data_blob_sz,   8);
        write_le!(self.p4,  8);
        write_le!(self.flags,          8);
        write_le!(self.seek_pk_lo,     8);  // bytes 80-87
        write_le!(self.seek_pk_hi,     8);  // bytes 88-95
        buf
    }

    pub fn unpack(buf: &[u8]) -> Result<Self, ProtocolError> {
        if buf.len() < HEADER_SIZE {
            return Err(ProtocolError::DecodeError(format!(
                "header too short: {} < {}", buf.len(), HEADER_SIZE
            )));
        }

        macro_rules! read_le_u64 {
            ($off:expr) => {
                u64::from_le_bytes(buf[$off..$off + 8].try_into().unwrap())
            };
        }
        macro_rules! read_le_u32 {
            ($off:expr) => {
                u32::from_le_bytes(buf[$off..$off + 4].try_into().unwrap())
            };
        }

        let magic = read_le_u64!(0);
        if magic != MAGIC_V2 {
            return Err(ProtocolError::BadMagic(magic));
        }

        Ok(Header {
            magic,
            status:         read_le_u32!(8),
            err_len:        read_le_u32!(12),
            target_id:      read_le_u64!(16),
            client_id:      read_le_u64!(24),
            schema_count:   read_le_u64!(32),
            schema_blob_sz: read_le_u64!(40),
            data_count:     read_le_u64!(48),
            data_blob_sz:   read_le_u64!(56),
            p4:  read_le_u64!(64),
            flags:          read_le_u64!(72),
            seek_pk_lo:     read_le_u64!(80),
            seek_pk_hi:     read_le_u64!(88),
        })
    }
}

impl Header {
    /// Set p4/p5/p6 for FLAG_SEEK_BY_INDEX requests.
    pub fn set_seek_by_index(&mut self, col_idx: u64, key_lo: u64, key_hi: u64) {
        self.p4 = col_idx;
        self.seek_pk_lo    = key_lo;
        self.seek_pk_hi    = key_hi;
    }
    pub fn seek_col_idx(&self)    -> u64 { self.p4 }
    pub fn seek_idx_key_lo(&self) -> u64 { self.seek_pk_lo }
    pub fn seek_idx_key_hi(&self) -> u64 { self.seek_pk_hi }
}

impl Default for Header {
    fn default() -> Self {
        Header {
            magic:          MAGIC_V2,
            status:         0,
            err_len:        0,
            target_id:      0,
            client_id:      0,
            schema_count:   0,
            schema_blob_sz: 0,
            data_count:     0,
            data_blob_sz:   0,
            p4:  0,
            flags:          0,
            seek_pk_lo:     0,
            seek_pk_hi:     0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_pack_unpack() {
        let h = Header {
            magic:          MAGIC_V2,
            status:         STATUS_ERROR,
            err_len:        42,
            target_id:      0x1122_3344_5566_7788,
            client_id:      0xAABB_CCDD_EEFF_0011,
            schema_count:   3,
            schema_blob_sz: 256,
            data_count:     100,
            data_blob_sz:   4096,
            p4:  42,
            flags:          FLAG_PUSH | FLAG_HAS_PK,
            seek_pk_lo:     0xDEAD_BEEF_1234_5678,
            seek_pk_hi:     0x1111_2222_3333_4444,
        };

        let packed = h.pack();
        assert_eq!(packed.len(), HEADER_SIZE);

        let h2 = Header::unpack(&packed).unwrap();
        assert_eq!(h2.magic,          h.magic);
        assert_eq!(h2.status,         h.status);
        assert_eq!(h2.err_len,        h.err_len);
        assert_eq!(h2.target_id,      h.target_id);
        assert_eq!(h2.client_id,      h.client_id);
        assert_eq!(h2.schema_count,   h.schema_count);
        assert_eq!(h2.schema_blob_sz, h.schema_blob_sz);
        assert_eq!(h2.data_count,     h.data_count);
        assert_eq!(h2.data_blob_sz,   h.data_blob_sz);
        assert_eq!(h2.p4,  h.p4);
        assert_eq!(h2.flags,          h.flags);
        assert_eq!(h2.seek_pk_lo,     h.seek_pk_lo);
        assert_eq!(h2.seek_pk_hi,     h.seek_pk_hi);
    }

    #[test]
    fn test_header_bad_magic() {
        let mut packed = Header::default().pack();
        packed[0] = 0xFF; // corrupt magic
        let res = Header::unpack(&packed);
        assert!(matches!(res, Err(ProtocolError::BadMagic(_))));
    }

    #[test]
    fn test_header_too_short() {
        let res = Header::unpack(&[0u8; 10]);
        assert!(matches!(res, Err(ProtocolError::DecodeError(_))));
    }
}
