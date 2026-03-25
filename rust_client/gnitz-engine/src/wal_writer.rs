//! WAL writer and reader for durable batch persistence.
//!
//! Appends columnar WAL blocks to a file. Reads them back for recovery.
//! Reuses wal.rs encode/validate_and_parse for the binary format.

use std::ffi::CString;

use crate::shard_reader::MappedShard;
use crate::compact::SchemaDescriptor;
use crate::wal;

const MAX_REGIONS: usize = 128;

pub struct WalWriter {
    fd: i32,
    buf: Vec<u8>,
    buf_offset: usize,
}

impl WalWriter {
    pub fn open(path: &str) -> Result<Self, i32> {
        let cpath = CString::new(path).map_err(|_| -1)?;
        let fd = unsafe {
            libc::open(
                cpath.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
                0o644,
            )
        };
        if fd < 0 { return Err(-1); }

        Ok(WalWriter {
            fd,
            buf: vec![0u8; 4 * 1024 * 1024], // 4MB write buffer
            buf_offset: 0,
        })
    }

    /// Append a batch (described by a MappedShard view) as a WAL block.
    pub fn append_batch(
        &mut self,
        lsn: u64,
        table_id: u32,
        src: &MappedShard,
        schema: &SchemaDescriptor,
    ) -> Result<(), i32> {
        let count = src.count;
        if count == 0 { return Ok(()); }

        let pk_index = schema.pk_index as usize;
        let num_cols = schema.num_columns as usize;

        // Gather region pointers + sizes
        let mut region_ptrs: Vec<*const u8> = Vec::new();
        let mut region_sizes: Vec<u32> = Vec::new();

        region_ptrs.push(src.pk_lo_ptr);
        region_sizes.push((count * 8) as u32);
        region_ptrs.push(src.pk_hi_ptr);
        region_sizes.push((count * 8) as u32);
        region_ptrs.push(src.weight_ptr);
        region_sizes.push((count * 8) as u32);
        region_ptrs.push(src.null_ptr);
        region_sizes.push((count * 8) as u32);

        let mut payload_idx = 0usize;
        for ci in 0..num_cols {
            if ci == pk_index { continue; }
            let col_size = schema.columns[ci].size as usize;
            region_ptrs.push(src.col_ptrs[payload_idx]);
            region_sizes.push((count * col_size) as u32);
            payload_idx += 1;
        }

        region_ptrs.push(src.blob_ptr_);
        region_sizes.push(src.blob_len() as u32);

        let blob_size = src.blob_len() as u64;
        let num_regions = region_ptrs.len();

        // Encode WAL block
        let needed = wal::estimate_block_size(count as u32, &region_sizes, blob_size);
        if self.buf.len() < self.buf_offset + needed {
            self.buf.resize(self.buf_offset + needed + 4096, 0);
        }

        let new_offset = wal::encode(
            &mut self.buf, self.buf_offset,
            lsn, table_id, count as u32,
            &region_ptrs, &region_sizes, blob_size,
        );

        if new_offset < 0 { return Err(-3); }

        // Write to file
        let write_len = new_offset as usize - self.buf_offset;
        let data = &self.buf[self.buf_offset..self.buf_offset + write_len];
        let mut written = 0usize;
        while written < write_len {
            let n = unsafe {
                libc::write(
                    self.fd,
                    data[written..].as_ptr() as *const libc::c_void,
                    write_len - written,
                )
            };
            if n < 0 {
                let e = unsafe { *libc::__errno_location() };
                if e == libc::EINTR { continue; }
                return Err(-3);
            }
            if n == 0 { return Err(-3); }
            written += n as usize;
        }

        if unsafe { libc::fdatasync(self.fd) } < 0 {
            return Err(-4);
        }

        // Reset buffer offset for next block (reuse buffer from start)
        self.buf_offset = 0;

        Ok(())
    }

    pub fn truncate_before_lsn(&mut self, _boundary_lsn: u64) {
        // Simple truncation: just truncate the file to 0
        // (the RPython WAL does the same — WAL is replayed from the boundary LSN)
        unsafe { libc::ftruncate(self.fd, 0); }
    }

    pub fn close(&mut self) {
        if self.fd >= 0 {
            unsafe { libc::close(self.fd); }
            self.fd = -1;
        }
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        self.close();
    }
}

/// Read WAL blocks for recovery. Returns parsed blocks as (lsn, table_id, MappedShard view).
/// The MappedShard views borrow from the file data buffer — caller must keep it alive.
pub struct WalBlock {
    pub lsn: u64,
    pub table_id: u32,
    pub count: u32,
    /// Raw block data (owned). The MappedShard from decode borrows this.
    pub data: Vec<u8>,
    pub region_offsets: Vec<u32>,
    pub region_sizes: Vec<u32>,
    pub num_regions: u32,
    pub blob_size: u64,
}

pub fn read_wal_blocks(path: &str) -> Vec<WalBlock> {
    let cpath = match CString::new(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
    if fd < 0 { return Vec::new(); }

    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstat(fd, &mut st) } < 0 {
        unsafe { libc::close(fd); }
        return Vec::new();
    }
    let file_size = st.st_size as usize;
    if file_size == 0 {
        unsafe { libc::close(fd); }
        return Vec::new();
    }

    let mut file_data = vec![0u8; file_size];
    let mut read_pos = 0usize;
    while read_pos < file_size {
        let n = unsafe {
            libc::read(
                fd,
                file_data[read_pos..].as_mut_ptr() as *mut libc::c_void,
                file_size - read_pos,
            )
        };
        if n <= 0 { break; }
        read_pos += n as usize;
    }
    unsafe { libc::close(fd); }

    let mut blocks = Vec::new();
    let mut offset = 0usize;

    while offset < read_pos {
        let remaining = &file_data[offset..read_pos];
        let block_len = remaining.len() as i64;

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut r_offsets = [0u32; MAX_REGIONS];
        let mut r_sizes = [0u32; MAX_REGIONS];

        let rc = wal::validate_and_parse(
            remaining, &mut lsn, &mut tid, &mut count,
            &mut num_regions, &mut blob_size,
            &mut r_offsets, &mut r_sizes, MAX_REGIONS as u32,
        );

        if rc != 0 { break; } // corrupt or truncated — stop

        // Compute block size from last region
        let nr = num_regions as usize;
        let block_end = if nr > 0 {
            (r_offsets[nr - 1] + r_sizes[nr - 1]) as usize
        } else {
            0
        };

        // The block data starts at `offset` in file_data
        let block_data = file_data[offset..offset + block_end.max(64)].to_vec();

        blocks.push(WalBlock {
            lsn,
            table_id: tid,
            count,
            data: block_data,
            region_offsets: r_offsets[..nr].to_vec(),
            region_sizes: r_sizes[..nr].to_vec(),
            num_regions,
            blob_size,
        });

        // Advance past this block — need the actual encoded block size
        // The WAL header is fixed-size, then region directory, then regions.
        // Use the block_end as the encoded size.
        offset += block_end.max(64); // at least skip the header
    }

    blocks
}

