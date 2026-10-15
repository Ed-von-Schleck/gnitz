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
use gnitz_wire::control::{peek_control_block, DecodedControl};
use gnitz_wire::fields::{self, Field, List, Section};
use gnitz_wire::wal;
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
    /// structure decoded out of a blob, a list or a string.
    pub fields: Vec<Field>,
    /// Every cell of every data block's fixed regions — the rows, where
    /// `fields` are the structure around them.
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
        let ((), map) = fields::map(frame, || {
            if let Ok(ctrl) = peek_control_block(frame) {
                walk_frame(frame, frame, ctrl, &mut anatomy);
            }
        });
        anatomy.fields = map.fields;
        anatomy.sections = map.sections;
        anatomy.lists = map.lists;
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
        let encoded = self.reencode(frame, s.block, s.cell, &content);
        spliced(frame, &self.sections, self.blocks[s.block].range.clone(), &encoded)
    }

    /// Data block `b` of `frame` again, the string whose cell starts at `cell`
    /// holding `content`, and every string's content rehomed into a fresh
    /// heap. A cell the anatomy holds no string for keeps its bytes.
    fn reencode(&self, frame: &[u8], b: usize, cell: usize, content: &[u8]) -> Vec<u8> {
        let block = &self.blocks[b];
        let layout = block.schema.layout();
        let (rows, fixed, _, _) =
            wal::parse_block(&frame[block.range.clone()], layout.row_width()).expect("a block the anatomy read");
        let fixed_at = offset(frame, fixed);
        let mut fixed = fixed.to_vec();
        let mut heap = Vec::new();
        for s in self.strings.iter().filter(|s| s.block == b) {
            let text = if s.cell == cell {
                content
            } else {
                &frame[s.content.clone()]
            };
            let at = s.cell - fixed_at;
            fixed[at..at + 16].copy_from_slice(&gnitz_wire::encode_german_string(text, &mut heap));
        }
        let mut regions = gnitz_wire::Regions::new();
        for r in 0..layout.num_regions() {
            let start = layout.region_start(r, rows);
            regions.push(&fixed[start..start + rows * layout.region_stride(r)]);
        }
        regions.push(&heap);
        let mut out = Vec::new();
        wal::append_block(&regions, 0, &mut out);
        out
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

/// Where `part`, a slice of `root`, starts in it.
fn offset(root: &[u8], part: &[u8]) -> usize {
    part.as_ptr().addr() - root.as_ptr().addr()
}

/// Map `frame`, a slice of `root` whose control block is `ctrl`, and the
/// structures nested in it.
fn walk_frame(root: &[u8], frame: &[u8], ctrl: DecodedControl, anatomy: &mut Anatomy) {
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
        None if verb == ClientVerb::DdlTxn => gnitz_wire::sys_family_index(ctrl.hdr.target_id)
            .is_some()
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
    // A scan's and a poll's items carry a read spec in their blob; the other
    // verbs' items are frames of their own.
    match verb {
        ClientVerb::ScanMulti => {
            if let Ok((scans, _)) = fields::attempt(|| txn_frame::decode_scan_multi(body)) {
                scans
                    .iter()
                    .for_each(|s| drop(fields::attempt(|| ReadSpec::decode(s.spec))));
            }
        }
        ClientVerb::DeltaPoll => {
            if let Ok((polls, _)) = fields::attempt(|| txn_frame::decode_delta_items(body)) {
                polls
                    .iter()
                    .for_each(|p| drop(fields::attempt(|| ReadSpec::decode(p.spec))));
            }
        }
        _ => {
            if let Ok((items, _)) = fields::attempt(|| txn_frame::decode_items(body, verb)) {
                for (item, ctrl) in items {
                    walk_frame(root, item, ctrl, anatomy);
                }
            }
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
    let fixed_at = offset(root, fixed);
    let b = anatomy.blocks.len();
    let at = offset(root, block);
    let mut cell = |at: usize, width: usize| anatomy.cells.push(Field { at, width });
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
    for pi in gnitz_wire::BitIter(layout.string_payload_slots()) {
        let start = layout.region_start(REG_PAYLOAD_START + pi, rows);
        for row in 0..rows {
            let cell = &fixed[start + row * 16..][..16];
            let content = match gnitz_wire::german_string_inline(cell) {
                Some(text) => text,
                // A long cell that overruns the heap is no string.
                None => match gnitz_wire::german_string_heap(cell, heap.len()) {
                    Some(r) => &heap[r],
                    None => continue,
                },
            };
            let content_at = offset(root, content);
            strings.push(StringCell {
                block: b,
                cell: offset(root, cell),
                content: content_at..content_at + content.len(),
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
