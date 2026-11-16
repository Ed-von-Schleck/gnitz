//! Maps: where each output payload slot comes from — a column copied verbatim or
//! a register the program computes — and [`MapEval`], which writes them.

use gnitz_wire::{FixedInt, TypeCode};

use crate::morsel::Evaluator;
use crate::program::{ReadAs, locate_col};
use crate::{
    BatchView, ColCopy, ColKind, ColMoves, ExprValidateErr, LogicalInstr, LogicalProgram, MapTarget, SchemaDescriptor,
    Sink,
};

/// A computed output slot written from a register.
#[derive(Debug, PartialEq)]
pub(crate) struct Emit {
    pub(crate) reg: usize,
    pub(crate) slot: usize,
    pub(crate) store: EmitStore,
}

/// How an emit stores its register into its slot: the value's low bytes at the
/// slot's width, a BOOLEAN slot's truth byte, or a German-string cell (`Str`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum EmitStore {
    W1,
    W2,
    W4,
    W8,
    Truth,
    Str,
}

impl EmitStore {
    /// How a scalar register is stored into a slot of `type_code`. A BOOLEAN
    /// slot takes any scalar register, read as every condition reads one: true
    /// where non-zero. Any other takes the whole 8-byte image, or the low bytes
    /// for a slot whose source range-checked the value into exactly the slot's
    /// integer type.
    pub(crate) fn for_slot(type_code: TypeCode, src: &LogicalInstr) -> Option<Self> {
        if type_code == TypeCode::Bool {
            return Some(EmitStore::Truth);
        }
        if type_code.wire_stride() == 8 {
            return Some(EmitStore::W8);
        }
        let fi = FixedInt::from_type_code(type_code).filter(|&fi| src.range_check() == Some(fi))?;
        match fi.width() {
            1 => Some(EmitStore::W1),
            2 => Some(EmitStore::W2),
            4 => Some(EmitStore::W4),
            _ => None,
        }
    }

    pub(crate) fn bytes(self) -> usize {
        match self {
            EmitStore::W1 | EmitStore::Truth => 1,
            EmitStore::W2 => 2,
            EmitStore::W4 => 4,
            EmitStore::W8 => 8,
            EmitStore::Str => 16,
        }
    }
}

impl LogicalProgram {
    /// A pure projection: `copies[i] = src_col` copies logical input column
    /// `src_col` into output payload slot `i`. The source type is derived in
    /// `map_sinks` from the schema. The instruction-free shape a map consumer
    /// turns into verbatim column moves and nothing else.
    pub fn copy_cols(copies: &[u32]) -> Self {
        let sinks = copies.iter().map(|&src_col| Sink::Col(src_col)).collect();
        Self::new(Vec::new(), sinks, Vec::new()).expect("a column copy reads no register and no pool entry")
    }

    /// A map from `in_schema` to `out_schema`, covering every output payload slot
    /// exactly once.
    pub fn resolve_map(
        &self,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Result<MapEval, ExprValidateErr> {
        let prog = self.resolve_program(in_schema, ReadAs::Value)?;
        let (moves, emits) = self.map_sinks(in_schema, out_schema)?;
        // The map reproduces its input, with the input PK carried through: one
        // layout on both sides, which fixes equal payload counts and types, and
        // every slot copied from its own input slot. With no emits copy `i`
        // writes slot `i`, and the program is never run, so instructions no
        // sink reads do not count against it.
        let is_identity = emits.is_empty()
            && in_schema.same_layout(out_schema)
            && moves
                .copies()
                .iter()
                .enumerate()
                .all(|(i, c)| c.src.payload_slot() == Some(i));
        Ok(MapEval {
            ev: Evaluator::new(prog),
            moves,
            emits,
            is_identity,
        })
    }

    /// Hold this map's sinks to its two schemas and say where each output slot
    /// comes from: the copied ones as moves, the computed ones as emits in slot
    /// order. Sink `i` writes slot `i`, so covering every declared payload slot
    /// is a count.
    fn map_sinks(
        &self,
        in_schema: &SchemaDescriptor,
        out_schema: &SchemaDescriptor,
    ) -> Result<(ColMoves, Vec<Emit>), ExprValidateErr> {
        use ExprValidateErr as E;
        let num_payload_cols = out_schema.num_payload_cols();
        if self.sinks().len() != num_payload_cols {
            return Err(E::OutputSlotCountMismatch {
                sinks: self.sinks().len(),
                num_payload_cols,
            });
        }
        let out_slots = out_schema.payload_locator_iter();
        let (mut copies, mut emits) = (Vec::new(), Vec::new());
        for (slot, (sink, out_loc)) in self.sinks().iter().zip(out_slots).enumerate() {
            let (out, type_code) = (slot as u32, out_loc.type_code());
            match *sink {
                Sink::Col(col) => {
                    let src = locate_col(in_schema, col, ColKind::AnyCol)?;
                    let src_tc = src.type_code();
                    // Verbatim, or a narrower integer widened.
                    if src_tc != type_code && !src_tc.is_widening_promotion(type_code) {
                        return Err(E::CopyTypeMismatch { col, src_tc, out, out_tc: type_code });
                    }
                    copies.push(ColCopy { src, slot, width: out_loc.size() });
                }
                Sink::Reg(r) if self.is_str(r) != type_code.is_german_string() => {
                    return Err(E::EmitClassMismatch { out, type_code });
                }
                Sink::Reg(r) if self.is_str(r) => emits.push(Emit {
                    reg: r.0 as usize,
                    slot,
                    store: EmitStore::Str,
                }),
                Sink::Reg(r) => {
                    let store = EmitStore::for_slot(type_code, &self.instrs()[r.0 as usize])
                        .ok_or(E::EmitSlotWidth { out, type_code })?;
                    emits.push(Emit { reg: r.0 as usize, slot, store });
                }
            }
        }
        Ok((ColMoves::new(copies, in_schema.nullable_payload_slots()), emits))
    }
}

/// A map: column moves the caller runs, and the null words and computed columns
/// [`Self::write_computed`] writes.
pub struct MapEval {
    ev: Evaluator,
    moves: ColMoves,
    /// The computed slots, in slot order.
    emits: Vec<Emit>,
    is_identity: bool,
}

impl MapEval {
    /// The columns this map copies verbatim.
    pub fn moves(&self) -> &ColMoves {
        &self.moves
    }

    /// True iff some output slot is computed rather than copied.
    pub fn emits_anything(&self) -> bool {
        !self.emits.is_empty()
    }

    /// True iff some computed output slot is a German string, whose cell may spill into the
    /// output heap.
    pub fn emits_a_string(&self) -> bool {
        self.emits.iter().any(|e| e.store == EmitStore::Str)
    }

    /// True iff the map reproduces its input, given the input's PK carried through.
    pub fn is_identity(&self) -> bool {
        self.is_identity
    }

    /// Write the null words and computed columns of source rows
    /// `src_start..src_start + n` into `dst` rows from `dst_start`.
    pub fn write_computed(
        &mut self,
        src: &dyn BatchView,
        src_start: usize,
        n: usize,
        dst: &mut dyn MapTarget,
        dst_start: usize,
    ) {
        let MapEval { ev, moves, emits, .. } = self;
        moves.write_null_rows(src.null_bmp(), src_start, dst.null_bmp_mut(), dst_start, n);
        if emits.is_empty() {
            return;
        }
        ev.eval_morsels(src, src_start, n, |rel_start, out| {
            let row0 = dst_start + rel_start;
            for e in &*emits {
                out.emit(e, dst.slot_mut(e.slot), row0);
            }
        });
    }
}

#[cfg(test)]
impl crate::eval::Resolved for MapEval {
    fn ev(&self) -> Option<&Evaluator> {
        Some(&self.ev)
    }
    fn ev_mut(&mut self) -> Option<&mut Evaluator> {
        Some(&mut self.ev)
    }
}

#[cfg(test)]
#[path = "tests/map.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/map.rs"]
mod bench;
