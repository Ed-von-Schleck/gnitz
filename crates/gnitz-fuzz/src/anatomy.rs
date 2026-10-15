//! A frame's anatomy: every field, section and list the production decoders
//! read of it, every data block with the schema its rows decode under, and
//! every string cell of those blocks — the structure an edit is placed by.
//!
//! The decoders draw it ([`gnitz_wire::fields`]); this module knows only where
//! one encoded structure nests inside another: items inside a multi-item
//! frame, data blocks inside frames, strings inside blocks, a circuit inside a
//! `circuits` row's string, and expression programs inside a circuit.

use std::ops::Range;
use std::sync::Arc;

use gnitz_core::{sys_schema, Schema};
use gnitz_expr::LogicalProgram;
use gnitz_wire::control::peek_control_block;
use gnitz_wire::fields::{self, Field, List, Section};
use gnitz_wire::wal::{self, WAL_HEADER_SIZE};
use gnitz_wire::{txn_frame, Circuit, ClientVerb, ReadSpec, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

/// One data block, and the schema its rows decode under.
#[derive(Clone, Debug)]
pub struct Block {
    pub range: Range<usize>,
    pub schema: Arc<Schema>,
}

/// One German string cell of a data block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StringCell {
    /// Its block, an index into [`Anatomy::blocks`].
    pub block: usize,
    /// Where its 16-byte cell starts.
    pub cell: usize,
    /// Its content: inside the cell, or in the block's heap.
    pub content: Range<usize>,
}

/// What the decoders read of one frame. Every range is an offset into it.
#[derive(Clone, Debug, Default)]
pub struct Anatomy {
    /// Every integer a decoder read: header words, and the words of every
    /// structure decoded out of a blob, a list or a string — none wider than
    /// eight bytes, a 16-byte one being its two halves.
    pub fields: Vec<Field>,
    /// Every cell of every data block's fixed regions, a 16-byte cell as its
    /// two halves — the rows, where `fields` are the structure around them.
    pub cells: Vec<Field>,
    /// Every length-prefixed section, a frame's blob among them.
    pub sections: Vec<Section>,
    pub lists: Vec<List>,
    pub blocks: Vec<Block>,
    pub strings: Vec<StringCell>,
}

impl Anatomy {
    /// The anatomy of the frame `frame`, as far as the decoders accept it.
    pub fn of(frame: &[u8]) -> Anatomy {
        let mut anatomy = Anatomy::default();
        let ((), map) = fields::map(frame, || walk_frame(frame, frame, &mut anatomy));
        // A 16-byte integer is edited as its two halves, as a 16-byte cell is.
        anatomy.fields = map
            .fields
            .into_iter()
            .flat_map(|f| match f.width {
                16 => vec![Field { at: f.at, width: 8 }, Field { at: f.at + 8, width: 8 }],
                _ => vec![f],
            })
            .collect();
        anatomy.sections.extend(map.sections);
        anatomy.lists = map.lists;
        // A frame's items are peeked twice, once by the item splitter.
        anatomy.fields.sort_by_key(|f| (f.at, f.width));
        anatomy.fields.dedup();
        anatomy.sections.sort_by_key(|s| (s.prefix, s.content.end));
        anatomy.sections.dedup();
        anatomy
    }

    /// `frame`, whose anatomy this is, with `range` replaced by `with` and
    /// every length enclosing it stated anew: the prefix of each section that
    /// holds it, and — for a range inside a string — the string's cell, whose
    /// block is encoded again around the new content.
    pub fn splice(&self, frame: &[u8], range: Range<usize>, with: &[u8]) -> Vec<u8> {
        let innermost = self
            .strings
            .iter()
            .filter(|s| holds(&s.content, &range))
            .min_by_key(|s| s.content.len());
        let Some(s) = innermost else {
            return spliced(frame, &self.sections, range, with);
        };
        let content = spliced(
            &frame[s.content.clone()],
            &self.sections_in(&s.content),
            shift(&range, s.content.start),
            with,
        );
        let block = &self.blocks[s.block];
        let encoded = reencode(
            &frame[block.range.clone()],
            &block.schema,
            s.cell - block.range.start,
            &content,
        );
        spliced(frame, &self.sections, block.range.clone(), &encoded)
    }

    /// The sections lying wholly inside `outer`, re-based onto its start.
    fn sections_in(&self, outer: &Range<usize>) -> Vec<Section> {
        self.sections
            .iter()
            .filter(|s| outer.start <= s.prefix && holds(outer, &s.content))
            .map(|s| Section {
                prefix: s.prefix - outer.start,
                content: shift(&s.content, outer.start),
            })
            .collect()
    }
}

/// Whether `outer` holds all of `inner`.
fn holds(outer: &Range<usize>, inner: &Range<usize>) -> bool {
    outer.start <= inner.start && inner.end <= outer.end
}

fn shift(r: &Range<usize>, by: usize) -> Range<usize> {
    r.start - by..r.end - by
}

/// `bytes` with `range` replaced by `with`, and the prefix of every one of
/// `sections` that holds `range` moved by the change in length.
fn spliced(bytes: &[u8], sections: &[Section], range: Range<usize>, with: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    let delta = with.len() as i64 - range.len() as i64;
    for s in sections.iter().filter(|s| holds(&s.content, &range)) {
        let p = &mut out[s.prefix..s.prefix + 4];
        let len = u32::from_le_bytes(p.try_into().unwrap()) as i64 + delta;
        p.copy_from_slice(&(len as u32).to_le_bytes());
    }
    out.splice(range, with.iter().copied());
    out
}

/// The data block `block` of `schema` again, the string whose cell starts at
/// `cell` (an offset into the block) holding `content`, and every string's
/// content rehomed into a fresh heap.
fn reencode(block: &[u8], schema: &Schema, cell: usize, content: &[u8]) -> Vec<u8> {
    let layout = schema.layout();
    let (rows, fixed, heap, _) = wal::parse_block(block, layout.row_width()).expect("a block the anatomy read");
    let mut fixed = fixed.to_vec();
    let mut new_heap = Vec::new();
    for pi in string_slots(schema) {
        let start = layout.region_start(REG_PAYLOAD_START + pi, rows);
        for row in 0..rows {
            let at = start + row * 16;
            let old: [u8; 16] = fixed[at..at + 16].try_into().unwrap();
            let text = if WAL_HEADER_SIZE + at == cell {
                content
            } else {
                match string_content(&old, heap) {
                    Some(r) if gnitz_wire::german_string_inline(&old).is_some() => &old[r],
                    Some(r) => &heap[r],
                    // A cell no encoder made keeps its bytes.
                    None => continue,
                }
            };
            let new = gnitz_wire::encode_german_string(text, &mut new_heap);
            fixed[at..at + 16].copy_from_slice(&new);
        }
    }
    let mut regions = gnitz_wire::Regions::new();
    for r in 0..layout.num_regions() {
        let start = layout.region_start(r, rows);
        regions.push(&fixed[start..start + rows * layout.region_stride(r)]);
    }
    regions.push(&new_heap);
    let mut out = Vec::new();
    wal::append_block(&regions, 0, &mut out);
    out
}

/// The payload slots of `schema` that hold German strings.
fn string_slots(schema: &Schema) -> impl Iterator<Item = usize> {
    let slots = schema.layout().string_payload_slots();
    (0..64).filter(move |pi| slots >> pi & 1 != 0)
}

/// Where `cell`'s content lies: inside the cell itself for a short one, in
/// `heap` for a long one; `None` for a long cell that overruns the heap.
fn string_content(cell: &[u8; 16], heap: &[u8]) -> Option<Range<usize>> {
    match gnitz_wire::german_string_inline(cell) {
        Some(text) => Some(gnitz_wire::GERMAN_INLINE_OFF..gnitz_wire::GERMAN_INLINE_OFF + text.len()),
        None => gnitz_wire::german_string_heap(cell, heap.len()),
    }
}

/// Where `part`, a slice of `root`, starts in it.
fn offset(root: &[u8], part: &[u8]) -> usize {
    part.as_ptr().addr() - root.as_ptr().addr()
}

/// Map `frame`, a slice of `root`, and the structures nested in it.
fn walk_frame(root: &[u8], frame: &[u8], anatomy: &mut Anatomy) {
    let Ok(ctrl) = peek_control_block(frame) else { return };
    let base = offset(root, frame);
    anatomy.sections.push(Section {
        prefix: base + 4,
        content: base + ctrl.blob.start..base + ctrl.blob.end,
    });
    let verb = ctrl.hdr.flags.verb;
    let blob = &frame[ctrl.blob.clone()];
    match verb {
        ClientVerb::ScanSpec => drop(fields::attempt(|| ReadSpec::decode(blob))),
        ClientVerb::SyncPushed => drop(fields::attempt(|| txn_frame::decode_held(blob))),
        _ => {}
    }
    let schema = match &ctrl.schema {
        Some(s) => fields::attempt(|| Schema::from_block(&frame[s.clone()]))
            .ok()
            .map(|(s, _)| Arc::new(s)),
        // A DDL_TXN item's rows are its system family's, which the item names.
        None if verb == ClientVerb::DdlTxn => gnitz_wire::SYS_FAMILIES
            .iter()
            .any(|f| f.id == ctrl.hdr.target_id)
            .then(|| Arc::clone(sys_schema(ctrl.hdr.target_id))),
        None => None,
    };
    if let (Some(data), Some(schema)) = (ctrl.data.clone(), schema) {
        walk_block(root, &frame[data], schema, anatomy);
    }
    let body = &frame[ctrl.body.clone()];
    if body.is_empty() {
        return;
    }
    match verb {
        ClientVerb::ScanMulti => drop(fields::attempt(|| txn_frame::decode_scan_multi(body))),
        ClientVerb::DeltaPoll => drop(fields::attempt(|| txn_frame::decode_delta_items(body))),
        _ => {}
    }
    if let Ok((items, _)) = fields::attempt(|| txn_frame::decode_items(body, verb)) {
        for (item, _) in items {
            walk_frame(root, item, anatomy);
        }
    }
}

/// Map the data block `block` of `schema`: its header, every cell, every
/// string and what decodes inside one.
fn walk_block(root: &[u8], block: &[u8], schema: Arc<Schema>, anatomy: &mut Anatomy) {
    let layout = schema.layout();
    let Ok(((rows, fixed, heap, _), _)) = fields::attempt(|| wal::parse_block(block, layout.row_width())) else {
        return;
    };
    let (fixed_at, heap_at) = (offset(root, fixed), offset(root, heap));
    let b = anatomy.blocks.len();
    let at = offset(root, block);
    let mut cell = |at: usize, width: usize| match width {
        16 => anatomy
            .cells
            .extend([Field { at, width: 8 }, Field { at: at + 8, width: 8 }]),
        width => anatomy.cells.push(Field { at, width }),
    };
    for r in [REG_WEIGHT, REG_NULL_BMP] {
        let start = fixed_at + layout.region_start(r, rows);
        (0..rows).for_each(|row| cell(start + row * 8, 8));
    }
    let pk_start = fixed_at + layout.region_start(REG_PK, rows);
    for ci in 0..layout.num_columns() {
        match layout.locate(ci) {
            gnitz_expr::ColumnLocator::Pk { byte_off, size, .. } => {
                for row in 0..rows {
                    cell(pk_start + row * layout.pk_stride() + byte_off as usize, size as usize);
                }
            }
            gnitz_expr::ColumnLocator::Payload { slot, size, .. } => {
                let start = fixed_at + layout.region_start(REG_PAYLOAD_START + slot as usize, rows);
                (0..rows).for_each(|row| cell(start + row * size as usize, size as usize));
            }
        }
    }
    let mut strings = Vec::new();
    for pi in string_slots(&schema) {
        let start = layout.region_start(REG_PAYLOAD_START + pi, rows);
        for row in 0..rows {
            let c = start + row * 16;
            let bytes: &[u8; 16] = fixed[c..c + 16].try_into().unwrap();
            let Some(content) = string_content(bytes, heap) else {
                continue;
            };
            let base = if gnitz_wire::german_string_inline(bytes).is_some() {
                fixed_at + c
            } else {
                heap_at
            };
            strings.push(StringCell {
                block: b,
                cell: fixed_at + c,
                content: base + content.start..base + content.end,
            });
        }
    }
    anatomy.blocks.push(Block { range: at..at + block.len(), schema });
    for s in &strings {
        walk_string(root, s.content.clone());
    }
    anatomy.strings.extend(strings);
}

/// Map whatever decodes inside the string `root[content]`: a circuit, and the
/// expression programs among its sections.
fn walk_string(root: &[u8], content: Range<usize>) {
    let Ok((_, sections)) = fields::attempt(|| Circuit::decode(&root[content])) else {
        return;
    };
    for s in sections {
        let _ = fields::attempt(|| LogicalProgram::from_blob(&root[s.content]));
    }
}

#[cfg(test)]
#[path = "tests/anatomy.rs"]
mod tests;
