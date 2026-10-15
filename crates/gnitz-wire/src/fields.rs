//! The field map of an encoded byte string, as the decoders read it: every
//! integer a [`Reader`](crate::Reader) took, every `u32`-length-prefixed section
//! and every counted list, located by byte offset.
//!
//! Nothing here knows a format. [`map`] runs a decode with every reader over
//! the mapped bytes noting what it reads, so a map is exactly the structure the
//! production decoder accepted, and it moves with the decoder when a format
//! changes. A reader over bytes outside the mapped string, such as a copy,
//! notes nothing.
//!
//! Compiled only under the `field-map` feature: the fuzzer is its one user, and
//! a build without it reads no thread-local per field.

use std::cell::RefCell;
use std::ops::Range;

/// One fixed-width little-endian integer a reader took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Field {
    pub at: usize,
    pub width: usize,
}

impl Field {
    /// The field's bytes in the mapped string.
    pub fn range(&self) -> Range<usize> {
        self.at..self.at + self.width
    }
}

/// A section whose length a `u32` at `prefix` states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    pub prefix: usize,
    pub content: Range<usize>,
}

/// A counted list: its `u16` count at `count`, and each item's bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct List {
    pub count: usize,
    pub items: Vec<Range<usize>>,
}

/// What every decode under one [`map`] read of the mapped string.
#[derive(Clone, Debug, Default)]
pub struct FieldMap {
    pub fields: Vec<Field>,
    pub sections: Vec<Section>,
    pub lists: Vec<List>,
}

/// The mapping in progress: the mapped string's address range, the map, and
/// the lists still being read, innermost last.
struct Mapping {
    root: Range<usize>,
    map: FieldMap,
    open: Vec<List>,
}

thread_local! {
    static MAPPING: RefCell<Option<Mapping>> = const { RefCell::new(None) };
}

/// Run `decode` and return, beside its result, the map of what every reader
/// over `root` read during it. Maps do not nest.
pub fn map<T>(root: &[u8], decode: impl FnOnce() -> T) -> (T, FieldMap) {
    let addr = root.as_ptr().addr();
    MAPPING.with_borrow_mut(|m| {
        assert!(m.is_none(), "field maps do not nest");
        *m = Some(Mapping {
            root: addr..addr + root.len(),
            map: FieldMap::default(),
            open: Vec::new(),
        });
    });
    // Closes the mapping even when `decode` unwinds, so the next map starts clean.
    struct Close;
    impl Drop for Close {
        fn drop(&mut self) {
            MAPPING.with_borrow_mut(Option::take);
        }
    }
    let _close = Close;
    let out = decode();
    let mapping = MAPPING
        .with_borrow_mut(Option::take)
        .expect("the mapping is still open");
    (out, mapping.map)
}

/// Run `decode` under the open map, keeping what it read only when it
/// succeeds, and answer with the sections it read beside its value — what a
/// caller trying a decoder on bytes that may not be of its format needs.
/// Outside a map it only runs `decode`.
pub fn attempt<T, E>(decode: impl FnOnce() -> Result<T, E>) -> Result<(T, Vec<Section>), E> {
    let lens = || {
        MAPPING.with_borrow(|m| {
            m.as_ref()
                .map(|m| (m.map.fields.len(), m.map.sections.len(), m.map.lists.len()))
        })
    };
    let before = lens();
    let out = decode();
    MAPPING.with_borrow_mut(|m| {
        let (Some(m), Some((fields, sections, lists))) = (m, before) else {
            return out.map(|v| (v, Vec::new()));
        };
        match out {
            Ok(v) => Ok((v, m.map.sections[sections..].to_vec())),
            Err(e) => {
                m.map.fields.truncate(fields);
                m.map.sections.truncate(sections);
                m.map.lists.truncate(lists);
                m.open.clear();
                Err(e)
            }
        }
    })
}

/// Note through the open mapping, when `bytes` lie in its string: `f` gets
/// their offset in it.
fn note(bytes: &[u8], f: impl FnOnce(&mut Mapping, usize)) {
    MAPPING.with_borrow_mut(|m| {
        let Some(m) = m else { return };
        let addr = bytes.as_ptr().addr();
        if m.root.contains(&addr) || (bytes.is_empty() && addr == m.root.end) {
            let at = addr - m.root.start;
            f(m, at);
        }
    });
}

/// A reader took the integer `bytes`.
pub(crate) fn note_field(bytes: &[u8]) {
    note(bytes, |m, at| m.map.fields.push(Field { at, width: bytes.len() }));
}

/// A reader took the section `content` behind the `u32` prefix `prefix`.
pub(crate) fn note_section(prefix: &[u8], content: &[u8]) {
    note(prefix, |m, at| {
        let start = at + prefix.len();
        m.map.sections.push(Section {
            prefix: at,
            content: start..start + content.len(),
        });
    });
}

/// A reader is about to read a list whose count is `count`.
pub(crate) fn open_list(count: &[u8]) {
    note(count, |m, at| m.open.push(List { count: at, items: Vec::new() }));
}

/// The open list's next item spans `item`.
pub(crate) fn list_item(item: &[u8]) {
    note(item, |m, at| {
        if let Some(list) = m.open.last_mut() {
            list.items.push(at..at + item.len());
        }
    });
}

/// The open list read its last item.
pub(crate) fn close_list(count: &[u8]) {
    note(count, |m, at| {
        if let Some(pos) = m.open.iter().rposition(|l| l.count == at) {
            let list = m.open.remove(pos);
            m.map.lists.push(list);
        }
    });
}

#[cfg(test)]
#[path = "tests/fields.rs"]
mod tests;
