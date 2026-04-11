//! Secondary index integration: GiDesc, AviDesc, op_integrate_with_indexes.

use crate::schema::{SchemaDescriptor, SchemaColumn, type_code};
use crate::storage::{Batch, MemBatch};

// ---------------------------------------------------------------------------
// Helper: payload index
// ---------------------------------------------------------------------------

#[inline]
fn payload_idx(col_idx: usize, pk_index: usize) -> usize {
    if col_idx < pk_index { col_idx } else { col_idx - 1 }
}

// ---------------------------------------------------------------------------
// Column promotion helpers
// ---------------------------------------------------------------------------

/// Promote a column value to u64 for GroupIndex composite keys.
/// Ports group_index.py:26-42.
fn promote_col_to_u64(mb: &MemBatch, row: usize, col_idx: usize, pk_index: usize, col_type_code: u8) -> u64 {
    let pi = payload_idx(col_idx, pk_index);
    let cs = match col_type_code {
        type_code::U8 | type_code::I8 => 1,
        type_code::U16 | type_code::I16 => 2,
        type_code::U32 | type_code::I32 | type_code::F32 => 4,
        _ => 8, // U64, I64, F64; U128 excluded by caller
    };
    let ptr = mb.get_col_ptr(row, pi, cs);
    let mut buf = [0u8; 8];
    buf[..cs].copy_from_slice(ptr);
    u64::from_le_bytes(buf)
}

/// Promote an aggregate column value to an order-preserving u64 for AVI keys.
/// Ports group_index.py:169-192.
fn promote_agg_col_to_u64_ordered(
    mb: &MemBatch,
    row: usize,
    col_idx: usize,
    pk_index: usize,
    col_type_code: u8,
    for_max: bool,
) -> u64 {
    let pi = payload_idx(col_idx, pk_index);

    if col_type_code == type_code::F32 {
        let ptr = mb.get_col_ptr(row, pi, 4);
        let raw32 = u32::from_le_bytes(ptr.try_into().unwrap());
        let val = ieee_order_bits_f32(raw32);
        return if for_max { !val } else { val };
    }

    let cs = crate::schema::type_size(col_type_code) as usize;
    let ptr = mb.get_col_ptr(row, pi, cs);
    let mut buf = [0u8; 8];
    buf[..cs].copy_from_slice(ptr);
    let raw = u64::from_le_bytes(buf);

    let val = match col_type_code {
        type_code::U8 | type_code::U16 | type_code::U32 | type_code::U64 => raw,
        type_code::I8 | type_code::I16 | type_code::I32 | type_code::I64 => {
            // Sign-extend then offset-binary
            let signed = crate::schema::read_signed(&buf[..cs], cs);
            (signed as u64).wrapping_add(1u64 << 63)
        }
        type_code::F64 => ieee_order_bits(raw),
        _ => raw,
    };

    if for_max { !val } else { val }
}

fn ieee_order_bits(raw_bits: u64) -> u64 {
    if raw_bits >> 63 != 0 {
        !raw_bits
    } else {
        raw_bits ^ (1u64 << 63)
    }
}

fn ieee_order_bits_f32(raw_bits: u32) -> u64 {
    (if raw_bits >> 31 != 0 { !raw_bits } else { raw_bits ^ (1u32 << 31) }) as u64
}

/// Extract 64-bit group key for AVI composite keys.
fn extract_gc_u64(
    mb: &MemBatch,
    row: usize,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> u64 {
    let pki = schema.pk_index as usize;
    if group_by_cols.len() == 1 {
        let c_idx = group_by_cols[0] as usize;
        let tc = schema.columns[c_idx].type_code;
        if tc != type_code::U128 && tc != type_code::STRING
            && tc != type_code::F32 && tc != type_code::F64
        {
            let pi = payload_idx(c_idx, pki);
            let cs = crate::schema::type_size(tc) as usize;
            let ptr = mb.get_col_ptr(row, pi, cs);
            let mut buf = [0u8; 8];
            buf[..cs].copy_from_slice(ptr);
            return u64::from_le_bytes(buf);
        }
    }
    // Fallback: use extract_group_key from reduce (duplicate inline here)
    // For index.rs we only need the lo part.
    use crate::schema::type_code::STRING as TYPE_STRING;
    use crate::schema::SHORT_STRING_THRESHOLD;
    use crate::xxh;

    #[inline]
    fn mix64(mut v: u64) -> u64 {
        v ^= v >> 33;
        v = v.wrapping_mul(0xFF51AFD7ED558CCD);
        v ^= v >> 33;
        v = v.wrapping_mul(0xC4CEB9FE1A85EC53);
        v ^= v >> 33;
        v
    }

    let mut h: u64 = 0x9E3779B97F4A7C15;
    for (i, &c_idx_u32) in group_by_cols.iter().enumerate() {
        let c_idx = c_idx_u32 as usize;
        let tc = schema.columns[c_idx].type_code;
        let col_hash = if tc == TYPE_STRING {
            let pi = payload_idx(c_idx, pki);
            let struct_bytes = mb.get_col_ptr(row, pi, 16);
            let length = crate::util::read_u32_le(struct_bytes, 0) as usize;
            if length == 0 {
                0u64
            } else if length <= SHORT_STRING_THRESHOLD {
                xxh::checksum(&struct_bytes[4..4 + length])
            } else {
                let heap_offset = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
                xxh::checksum(&mb.blob[heap_offset..heap_offset + length])
            }
        } else {
            let pi = payload_idx(c_idx, pki);
            let cs = schema.columns[c_idx].size as usize;
            let ptr = mb.get_col_ptr(row, pi, cs);
            let mut buf = [0u8; 8];
            buf[..cs].copy_from_slice(ptr);
            mix64(u64::from_le_bytes(buf))
        };
        h = mix64(h ^ col_hash ^ (i as u64));
    }
    h
}

// ---------------------------------------------------------------------------
// Public descriptor types
// ---------------------------------------------------------------------------

/// GI/AVI descriptor for integrate_with_indexes.
pub struct GiDesc {
    pub table: *mut crate::storage::Table,
    pub col_idx: u32,
    pub col_type_code: u8,
}

pub struct AviDesc {
    pub table: *mut crate::storage::Table,
    pub for_max: bool,
    pub agg_col_type_code: u8,
    pub group_by_cols: Vec<u32>,
    pub input_schema: SchemaDescriptor,
    pub agg_col_idx: u32,
}

/// GI schema: U128 PK + I64 payload (spk_hi).
fn make_gi_schema() -> SchemaDescriptor {
    let mut s = SchemaDescriptor {
        num_columns: 2,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    s.columns[0] = SchemaColumn {
        type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
    };
    s.columns[1] = SchemaColumn {
        type_code: type_code::I64, size: 8, nullable: 0, _pad: 0,
    };
    s
}

/// AVI schema: U128 PK only, no payload.
fn make_avi_schema() -> SchemaDescriptor {
    let mut s = SchemaDescriptor {
        num_columns: 1,
        pk_index: 0,
        columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
    };
    s.columns[0] = SchemaColumn {
        type_code: type_code::U128, size: 16, nullable: 0, _pad: 0,
    };
    s
}

// ---------------------------------------------------------------------------
// op_integrate_with_indexes
// ---------------------------------------------------------------------------

/// Integrate a delta batch into a target table, optionally populating
/// GroupIndex and AggValueIndex secondary indexes.
///
/// Ports interpreter.py:171-221.  The Rust Table handles memtable capacity
/// internally (flush-on-overflow), so no explicit MemTableFullError retry.
pub fn op_integrate_with_indexes(
    batch: &Batch,
    target_table: Option<&mut crate::storage::Table>,
    input_schema: &SchemaDescriptor,
    gi: Option<&GiDesc>,
    avi: Option<&AviDesc>,
) -> i32 {
    if batch.count == 0 {
        return 0;
    }

    // Phase 1: ingest into target table
    if let Some(table) = target_table {
        let regions = batch.regions();
        let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
        let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
        let npc = input_schema.num_columns as usize - 1;
        if let Err(rc) = table.ingest_batch_memonly_from_regions(&ptrs, &sizes, batch.count as u32, npc) {
            return rc;
        }
    }

    let mb = batch.as_mem_batch();
    let pki = input_schema.pk_index as usize;

    // Phase 2: GroupIndex population (interpreter.py:175-199)
    if let Some(gi_desc) = gi {
        let gi_schema = make_gi_schema();
        let mut gi_batch = Batch::with_schema(gi_schema, batch.count);
        gi_batch.sorted = false;
        gi_batch.consolidated = false;

        let gi_col = gi_desc.col_idx as usize;
        let gi_pi = payload_idx(gi_col, pki);

        for row in 0..batch.count {
            // Skip null group column (interpreter.py:184)
            let null_word = mb.get_null_word(row);
            if (null_word >> gi_pi) & 1 != 0 {
                continue;
            }

            let gc_u64 = promote_col_to_u64(&mb, row, gi_col, pki, gi_desc.col_type_code);
            let source_pk = mb.get_pk(row);
            let source_pk_lo = source_pk as u64;
            let source_pk_hi = (source_pk >> 64) as u64;
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = source_pk_lo, ck_hi = gc_u64
            gi_batch.extend_pk_lo(&source_pk_lo.to_le_bytes());
            gi_batch.extend_pk_hi(&gc_u64.to_le_bytes());
            gi_batch.extend_weight(&weight.to_le_bytes());
            gi_batch.extend_null_bmp(&0u64.to_le_bytes());
            // Payload: spk_hi (source pk high 64 bits) as I64
            gi_batch.extend_col(0, &(source_pk_hi as i64).to_le_bytes());
            gi_batch.count += 1;
        }

        if gi_batch.count > 0 {
            let gi_table = unsafe { &mut *gi_desc.table };
            let gi_schema = gi_table.schema();
            let regions = gi_batch.regions();
            let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
            let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
            let _ = gi_table.ingest_batch_memonly_from_regions(
                &ptrs, &sizes, gi_batch.count as u32,
                gi_schema.num_columns as usize - 1,
            );
        }
    }

    // Phase 3: AggValueIndex population (interpreter.py:200-221)
    if let Some(avi_desc) = avi {
        let avi_schema = make_avi_schema();
        let mut avi_batch = Batch::with_schema(avi_schema, batch.count);
        avi_batch.sorted = false;
        avi_batch.consolidated = false;

        let avi_col = avi_desc.agg_col_idx as usize;
        let avi_pi = payload_idx(avi_col, pki);

        for row in 0..batch.count {
            // Skip null agg column (interpreter.py:208)
            let null_word = mb.get_null_word(row);
            if (null_word >> avi_pi) & 1 != 0 {
                continue;
            }

            let gc_u64 = extract_gc_u64(&mb, row, &avi_desc.input_schema, &avi_desc.group_by_cols);
            let av_u64 = promote_agg_col_to_u64_ordered(
                &mb, row, avi_col, pki,
                avi_desc.agg_col_type_code, avi_desc.for_max,
            );
            let weight = mb.get_weight(row);

            // Composite key: ck_lo = av_u64, ck_hi = gc_u64
            avi_batch.extend_pk_lo(&av_u64.to_le_bytes());
            avi_batch.extend_pk_hi(&gc_u64.to_le_bytes());
            avi_batch.extend_weight(&weight.to_le_bytes());
            avi_batch.extend_null_bmp(&0u64.to_le_bytes());
            // No payload columns (AVI schema is U128 PK only)
            avi_batch.count += 1;
        }

        if avi_batch.count > 0 {
            gnitz_debug!("integrate_avi: ingesting {} rows, for_max={}, agg_col_idx={}, agg_type={}",
                avi_batch.count, avi_desc.for_max, avi_desc.agg_col_idx, avi_desc.agg_col_type_code);
            for i in 0..avi_batch.count {
                let lo = u64::from_le_bytes(avi_batch.pk_lo_data()[i*8..(i+1)*8].try_into().unwrap());
                let hi = u64::from_le_bytes(avi_batch.pk_hi_data()[i*8..(i+1)*8].try_into().unwrap());
                let w = i64::from_le_bytes(avi_batch.weight_data()[i*8..(i+1)*8].try_into().unwrap());
                gnitz_debug!("  avi[{}]: pk_lo={:#x} pk_hi={:#x} w={}", i, lo, hi, w);
            }
            let avi_table = unsafe { &mut *avi_desc.table };
            let avi_schema = avi_table.schema();
            let regions = avi_batch.regions();
            let ptrs: Vec<*const u8> = regions.iter().map(|&(p, _)| p).collect();
            let sizes: Vec<u32> = regions.iter().map(|&(_, s)| s as u32).collect();
            let _ = avi_table.ingest_batch_memonly_from_regions(
                &ptrs, &sizes, avi_batch.count as u32,
                avi_schema.num_columns as usize - 1,
            );
        }
    }

    0
}
