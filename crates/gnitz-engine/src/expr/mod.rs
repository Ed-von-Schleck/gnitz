mod batch;
mod plan;
mod program;

#[cfg(test)]
mod tests;

// External (pub): appear in compiler/vm pub interfaces.
pub use plan::ScalarFunc;
pub use program::{LogicalProgram, ResolvedProgram};

pub(crate) use plan::FinalizeContext;
pub(crate) use program::{ColSrc, OutputColKind};

// Typed instruction model — consumed by hand-written test programs in the expr
// test suite and the #[cfg(test)] builders in vm/ops. Per-target test builds
// (lib vs bin) reference different subsets, so the import is liberally exposed
// under #[cfg(test)] with an allow for the unreferenced names in each build.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use program::{CmpOp, Instr, LogicalInstr, StrOp};
