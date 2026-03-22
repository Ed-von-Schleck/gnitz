#[derive(Clone, Debug, Copy, PartialEq)]
pub enum AggFunc { Count, CountNonNull, Sum, Min, Max, Avg }

#[derive(Clone, Debug)]
pub enum BoundExpr {
    ColRef(usize),
    LitInt(i64),
    LitFloat(f64),
    LitStr(String),
    BinOp(Box<BoundExpr>, BinOp, Box<BoundExpr>),
    UnaryOp(UnaryOp, Box<BoundExpr>),
    IsNull(usize),
    IsNotNull(usize),
    AggCall { func: AggFunc, arg: Option<Box<BoundExpr>> },
}

#[derive(Clone, Debug, Copy)]
pub enum BinOp { Add, Sub, Mul, Div, Mod, Eq, Ne, Gt, Ge, Lt, Le, And, Or }

#[derive(Clone, Debug, Copy)]
pub enum UnaryOp { Neg, Not }
