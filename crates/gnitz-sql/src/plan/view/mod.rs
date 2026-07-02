//! CREATE VIEW circuit builders, one per view shape. `dispatch` is the front
//! door (classify + route); `predicates` and `join` form the join cluster;
//! `group_by`, `set_op`, and `simple` cover the remaining shapes. Only
//! `execute_create_view` is exposed; everything else is internal to the cluster.

mod dispatch;
mod exists;
mod group_by;
mod join;
mod predicates;
mod set_op;
mod simple;

pub(crate) use dispatch::execute_create_view;
