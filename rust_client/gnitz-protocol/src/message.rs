use std::os::unix::io::RawFd;
use crate::error::ProtocolError;
use crate::header::{Header, HEADER_SIZE, ALIGNMENT, STATUS_ERROR};
use crate::types::{Schema, ZSetBatch, meta_schema};
use crate::codec::{align_up, encode_zset, layout, decode_zset, schema_to_batch, batch_to_schema};
use crate::transport::{send_memfd, recv_memfd};

pub struct Message {
    pub header:       Header,
    pub schema_batch: Option<ZSetBatch>,
    pub data_batch:   Option<ZSetBatch>,
    pub error_text:   Option<String>,   // Some(_) when header.status == STATUS_ERROR
}

pub fn send_message(
    sock_fd:          RawFd,
    mut header:       Header,
    schema:           Option<&Schema>,
    schema_batch_arg: Option<&ZSetBatch>,
    data_batch:       Option<&ZSetBatch>,
) -> Result<(), ProtocolError> {
    let ms = meta_schema();

    // 1. Determine effective schema batch
    let owned_sbatch: Option<ZSetBatch> = schema.map(schema_to_batch);
    let effective_schema_batch: Option<&ZSetBatch> = owned_sbatch.as_ref().or(schema_batch_arg);

    // 2. Encode schema section
    let (schema_bytes, schema_blob_sz) = match effective_schema_batch {
        Some(sbatch) => encode_zset(ms, sbatch),
        None         => (Vec::new(), 0u64),
    };
    let schema_count = effective_schema_batch.map(|b| b.len() as u64).unwrap_or(0);

    // 3. Encode data section
    let (data_bytes, data_blob_sz) = match (schema, data_batch) {
        (Some(s), Some(b)) => encode_zset(s, b),
        _                  => (Vec::new(), 0u64),
    };
    let data_count = data_batch.map(|b| b.len() as u64).unwrap_or(0);

    // 4. Populate header counts/sizes
    header.schema_count   = schema_count;
    header.schema_blob_sz = schema_blob_sz;
    header.data_count     = data_count;
    header.data_blob_sz   = data_blob_sz;
    // Only set p4 from schema when a schema is present.
    // If schema is None, the caller may have set p4 for another purpose
    // (e.g., col_idx for FLAG_SEEK_BY_INDEX), so preserve it.
    if let Some(s) = schema {
        header.p4 = s.pk_index as u64;
    }

    // 5. Assemble buffer
    let err_len    = header.err_len as usize;
    let body_start = align_up(HEADER_SIZE + err_len, ALIGNMENT);
    let schema_end = body_start + schema_bytes.len();
    let data_start = align_up(schema_end, ALIGNMENT);
    let total      = data_start + data_bytes.len();

    let mut buf = vec![0u8; total];
    buf[..HEADER_SIZE].copy_from_slice(&header.pack());

    if !schema_bytes.is_empty() {
        buf[body_start..body_start + schema_bytes.len()].copy_from_slice(&schema_bytes);
    }
    if !data_bytes.is_empty() {
        buf[data_start..data_start + data_bytes.len()].copy_from_slice(&data_bytes);
    }

    send_memfd(sock_fd, &buf)
}

pub fn recv_message(
    sock_fd:     RawFd,
    data_schema: Option<&Schema>,
) -> Result<Message, ProtocolError> {
    let buf = recv_memfd(sock_fd)?;
    let hdr = Header::unpack(&buf)?;

    if hdr.status == STATUS_ERROR {
        let err_end = (HEADER_SIZE + hdr.err_len as usize).min(buf.len());
        let text = std::str::from_utf8(&buf[HEADER_SIZE..err_end])
            .unwrap_or("<invalid utf8>")
            .to_string();
        return Ok(Message {
            header: hdr, schema_batch: None, data_batch: None, error_text: Some(text)
        });
    }

    let body_start = align_up(HEADER_SIZE + hdr.err_len as usize, ALIGNMENT);

    let mut schema_batch: Option<ZSetBatch> = None;
    let wire_schema: Option<Schema>;
    let data_start;

    if hdr.schema_count > 0 {
        let ms             = meta_schema();
        let schema_count   = hdr.schema_count as usize;
        let schema_blob_sz = hdr.schema_blob_sz as usize;

        let sbatch = decode_zset(&buf, body_start, ms, schema_count, schema_blob_sz)?;
        wire_schema = Some(batch_to_schema(&sbatch)?);

        let schema_section_size = layout(ms, schema_count).blob_off + schema_blob_sz;
        data_start  = align_up(body_start + schema_section_size, ALIGNMENT);
        schema_batch = Some(sbatch);
    } else {
        wire_schema = None;
        data_start  = body_start;
    }

    // Effective data schema: wire_schema takes precedence over caller-supplied schema
    let effective_schema: Option<&Schema> = match wire_schema.as_ref() {
        Some(s) => Some(s),
        None    => data_schema,
    };

    let data_batch: Option<ZSetBatch> = if let Some(ds) = effective_schema {
        let data_count   = hdr.data_count as usize;
        let data_blob_sz = hdr.data_blob_sz as usize;
        Some(decode_zset(&buf, data_start, ds, data_count, data_blob_sz)?)
    } else {
        None
    };

    Ok(Message { header: hdr, schema_batch, data_batch, error_text: None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::RawFd;
    use crate::types::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
    use crate::header::{Header, FLAG_PUSH};

    fn make_socketpair() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0, fds.as_mut_ptr()); }
        (fds[0], fds[1])
    }

    fn default_header() -> Header {
        Header { flags: FLAG_PUSH, ..Header::default() }
    }

    #[test]
    fn test_message_roundtrip_empty() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "val".into(), type_code: TypeCode::I64, is_nullable: false },
            ],
            pk_index: 0,
        };

        let empty_batch = ZSetBatch::new(&schema);
        let (a, b) = make_socketpair();
        send_message(a, default_header(), Some(&schema), None, Some(&empty_batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

        assert!(msg.data_batch.unwrap().is_empty());
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_roundtrip_data() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(),  type_code: TypeCode::U64, is_nullable: false },
                ColumnDef { name: "i64".into(), type_code: TypeCode::I64, is_nullable: false },
                ColumnDef { name: "f64".into(), type_code: TypeCode::F64, is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 100usize;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];
        let nulls: Vec<u64>   = vec![0; n];

        let i64_vals: Vec<i64> = (0..n as i64).map(|x| x * -7).collect();
        let f64_vals: Vec<f64> = (0..n).map(|x| x as f64 * 1.5).collect();

        let mut i64_bytes = Vec::with_capacity(n * 8);
        for &v in &i64_vals { i64_bytes.extend_from_slice(&v.to_le_bytes()); }
        let mut f64_bytes = Vec::with_capacity(n * 8);
        for &v in &f64_vals { f64_bytes.extend_from_slice(&v.to_le_bytes()); }

        let batch = ZSetBatch {
            pk_lo: pk_lo.clone(),
            pk_hi: pk_hi.clone(),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Fixed(i64_bytes.clone()),
                ColData::Fixed(f64_bytes.clone()),
            ],
        };

        let (a, b) = make_socketpair();
        send_message(a, default_header(), Some(&schema), None, Some(&batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

        let data = msg.data_batch.unwrap();
        assert_eq!(data.pk_lo,   pk_lo);
        assert_eq!(data.pk_hi,   pk_hi);
        assert_eq!(data.weights, weights);

        match &data.columns[1] {
            ColData::Fixed(got) => assert_eq!(got, &i64_bytes),
            _ => panic!("expected Fixed at col 1"),
        }
        match &data.columns[2] {
            ColData::Fixed(got) => assert_eq!(got, &f64_bytes),
            _ => panic!("expected Fixed at col 2"),
        }

        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_message_roundtrip_strings() {
        let schema = Schema {
            columns: vec![
                ColumnDef { name: "pk".into(), type_code: TypeCode::U64,    is_nullable: false },
                ColumnDef { name: "s1".into(), type_code: TypeCode::String, is_nullable: true  },
                ColumnDef { name: "s2".into(), type_code: TypeCode::String, is_nullable: false },
            ],
            pk_index: 0,
        };

        let n = 50usize;
        let pk_lo: Vec<u64>   = (0..n as u64).collect();
        let pk_hi: Vec<u64>   = vec![0; n];
        let weights: Vec<i64> = vec![1; n];
        // Every 3rd row: s1 is null → payload bit 0 set
        let nulls: Vec<u64>   = (0..n).map(|i| if i % 3 == 0 { 1u64 } else { 0u64 }).collect();

        let col1: Vec<Option<String>> = (0..n).map(|i| {
            if i % 3 == 0 { None } else { Some(format!("nullable_{}", i)) }
        }).collect();
        let col2: Vec<Option<String>> = (0..n).map(|i| Some(format!("nonnull_{}", i))).collect();

        let batch = ZSetBatch {
            pk_lo: pk_lo.clone(),
            pk_hi: pk_hi.clone(),
            weights: weights.clone(),
            nulls: nulls.clone(),
            columns: vec![
                ColData::Fixed(vec![]),
                ColData::Strings(col1.clone()),
                ColData::Strings(col2.clone()),
            ],
        };

        let (a, b) = make_socketpair();
        send_message(a, default_header(), Some(&schema), None, Some(&batch)).unwrap();
        let msg = recv_message(b, None).unwrap();

        let data = msg.data_batch.unwrap();
        assert_eq!(data.nulls, nulls);

        match &data.columns[1] {
            ColData::Strings(got) => assert_eq!(got, &col1),
            _ => panic!("expected Strings at col 1"),
        }
        match &data.columns[2] {
            ColData::Strings(got) => assert_eq!(got, &col2),
            _ => panic!("expected Strings at col 2"),
        }

        unsafe { libc::close(a); libc::close(b); }
    }
}
