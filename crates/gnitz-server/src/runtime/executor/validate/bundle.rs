//! A write bundle folded per table and key: what every rule of the pass reads.

use std::collections::hash_map::Entry;

use smallvec::SmallVec;

use super::violation::{Clash, pk_violation_err};
use super::*;

use crate::catalog::RowConstraints;
use crate::runtime::executor::family::TxnFamily;
use gnitz_wire::WireConflictMode;
use gnitz_zset::algebra::KeySpec;
use gnitz_zset::schema::SchemaDescriptor;

/// Per-PK last operation in a table's whole-bundle fold. `Inserted` names the
/// surviving row by `(family index into the bundle's family list, row index)`.
#[derive(Clone, Copy)]
pub(super) enum FoldOp {
    Inserted(u32, u32),
    Deleted,
}

/// One PK's fold over its table's families, in frame order.
pub(super) struct PkFold {
    /// The latest row op on the PK; once the bundle is folded, the op that survives.
    pub(super) last: FoldOp,
    /// An Error family's insert was the first op on the PK.
    pub(super) needs_probe: bool,
    /// A burst-1 probe found the PK committed.
    pub(super) committed: bool,
}

/// One table's fold, keyed by the row's OPK bytes.
pub(super) type Fold<'a> = FxHashMap<&'a [u8], PkFold>;

/// One table of the bundle a rule reads: its catalog schema, its constraints,
/// and its fold.
pub(super) struct TxnTable<'a> {
    pub(super) tid: u64,
    pub(super) schema: SchemaDescriptor,
    pub(super) cons: RowConstraints,
    pub(super) fold: Fold<'a>,
    /// The keys whose first op is an Error family's insert, in row order: a
    /// client's ascending keys reach the probe's sort ascending.
    pub(super) probe_keys: Vec<&'a [u8]>,
    /// The table's families, in bundle order.
    pub(super) fams: SmallVec<[u32; 1]>,
    /// Whether some PK is touched by two nonzero-weight rows. Where none is, the
    /// surviving rows are exactly the positive-weight ones.
    pub(super) repeats: bool,
}

/// A decoded transaction bundle plus everything its rules share. Built once;
/// every rule reads it instead of re-walking the families.
pub(super) struct TxnBundle<'a> {
    /// The rows of the bundle's families, indexed by family number. A push's
    /// one family is held inline.
    mems: SmallVec<[MemBatch<'a>; 1]>,
    /// The tables with an Error family or a constraint, in first-appearance
    /// order, found by linear scan over the write's own table count.
    pub(super) tables: Vec<TxnTable<'a>>,
}

impl<'a> TxnBundle<'a> {
    pub(super) fn new(cat: &CatalogEngine, families: &'a [TxnFamily]) -> Result<Self, WireFault> {
        let mut tables: Vec<TxnTable<'a>> = Vec::new();
        // Each table judged once, at its first family.
        for (i, fam) in families.iter().enumerate() {
            if families[..i].iter().any(|f| f.tid == fam.tid) {
                continue;
            }
            let reads_committed = families[i..]
                .iter()
                .any(|f| f.tid == fam.tid && cat.push_reads_committed_state(f.tid, f.mode));
            if !reads_committed {
                continue;
            }
            tables.push(TxnTable {
                tid: fam.tid,
                schema: cat.registry.relation_or_err(fam.tid)?.schema(),
                cons: cat.row_constraints(fam.tid),
                fold: Fold::default(),
                probe_keys: Vec::new(),
                fams: SmallVec::new(),
                repeats: false,
            });
        }
        let mems: SmallVec<[MemBatch<'a>; 1]> = families.iter().map(|f| f.rows.view()).collect();
        for t in &mut tables {
            let (tid, schema) = (t.tid, t.schema);
            for (fi, fam) in families.iter().enumerate().filter(|(_, f)| f.tid == tid) {
                let error = matches!(fam.mode, WireConflictMode::Error);
                let batch = mems[fi];
                t.fams.push(fi as u32);
                t.fold.reserve(batch.len());
                if error {
                    t.probe_keys.reserve(batch.len());
                }
                for row in 0..batch.len() {
                    let w = batch.get_weight(row);
                    if w == 0 {
                        continue;
                    }
                    let pk = batch.get_pk_bytes(row);
                    let op = if w > 0 {
                        FoldOp::Inserted(fi as u32, row as u32)
                    } else {
                        FoldOp::Deleted
                    };
                    let in_batch = || pk_violation_err(cat, tid, &schema, pk, Clash::InBatch);
                    let inserts = error && w > 0;
                    match t.fold.entry(pk) {
                        Entry::Occupied(o) => {
                            t.repeats = true;
                            let e = o.into_mut();
                            if inserts {
                                match e.last {
                                    FoldOp::Inserted(f, _) if f == fi as u32 => return Err(in_batch()),
                                    // Existence is cumulative in frame order, and a
                                    // cascade's families come later and only delete.
                                    FoldOp::Inserted(..) => {
                                        return Err(pk_violation_err(cat, tid, &schema, pk, Clash::Committed));
                                    }
                                    FoldOp::Deleted => {}
                                }
                            }
                            e.last = op;
                        }
                        Entry::Vacant(v) => {
                            v.insert(PkFold {
                                last: op,
                                needs_probe: inserts,
                                committed: false,
                            });
                            if inserts {
                                t.probe_keys.push(pk);
                            }
                        }
                    }
                    // `+w` is the row pushed `w` times: its second copy lands on its first.
                    if inserts && w > 1 {
                        return Err(in_batch());
                    }
                }
            }
        }
        Ok(TxnBundle { mems, tables })
    }

    pub(super) fn get(&self, tid: u64) -> Option<&TxnTable<'a>> {
        self.tables.iter().find(|t| t.tid == tid)
    }

    pub(super) fn get_mut(&mut self, tid: u64) -> Option<&mut TxnTable<'a>> {
        self.tables.iter_mut().find(|t| t.tid == tid)
    }

    pub(super) fn table(&self, tid: u64) -> &TxnTable<'a> {
        self.get(tid).expect("a bundled table")
    }

    /// Family `fam`'s rows.
    pub(super) fn mem(&self, fam: u32) -> &MemBatch<'a> {
        &self.mems[fam as usize]
    }

    /// The rows of `tid` that survive the whole bundle: `(pk, family, row)`.
    pub(super) fn surviving(&self, tid: u64) -> impl Iterator<Item = (&'a [u8], u32, u32)> {
        self.table(tid).fold.iter().filter_map(|(pk, e)| match e.last {
            FoldOp::Inserted(f, r) => Some((*pk, f, r)),
            FoldOp::Deleted => None,
        })
    }

    /// Whether the bundle retires `holder`'s claim on `span` in `tid`'s index
    /// `spec`: its surviving state is deleted, or holds a NULL or another span
    /// there. A holder the bundle does not touch, in a table it lists or not,
    /// keeps its claim.
    pub(super) fn retires(&self, tid: u64, spec: &KeySpec, holder: &[u8], span: &[u8]) -> bool {
        match self.get(tid).and_then(|t| t.fold.get(holder)).map(|e| e.last) {
            None => false,
            Some(FoldOp::Deleted) => true,
            Some(FoldOp::Inserted(f, r)) => spec
                .span(self.mem(f), r as usize)
                .is_none_or(|own| own.pk_bytes() != span),
        }
    }
}
