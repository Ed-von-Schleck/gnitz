mod batch;
mod plan;
mod program;

#[cfg(test)]
mod tests;

// The whole external surface: the VM/compiler builds a `LogicalProgram` from the
// wire blob and hands it to a `ScalarFunc` (filter, map, or projection). The
// resolved form and its instruction model never leave this module.
pub use plan::ScalarFunc;
pub use program::LogicalProgram;

// Logical instruction model — consumed by hand-written test programs in the expr
// test suite and the #[cfg(test)] builders in vm/ops. Per-target test builds
// (lib vs bin) reference different subsets, so the import is liberally exposed
// under #[cfg(test)] with an allow for the unreferenced names in each build.
// The resolved `Instr` never leaves this module.
pub(crate) use program::ExprValidateErr;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use program::{CmpOp, LogicalInstr, StrOp};
