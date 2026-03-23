use gnitz_protocol::{Schema, TypeCode};

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

impl BoundExpr {
    pub fn infer_type(&self, schema: &Schema) -> TypeCode {
        match self {
            BoundExpr::ColRef(idx) => schema.columns[*idx].type_code,
            BoundExpr::LitInt(_) => TypeCode::I64,
            BoundExpr::LitFloat(_) => TypeCode::F64,
            BoundExpr::LitStr(_) => TypeCode::String,
            BoundExpr::BinOp(l, op, r) => {
                let lt = l.infer_type(schema);
                let rt = r.infer_type(schema);
                match op {
                    BinOp::Eq | BinOp::Ne | BinOp::Gt | BinOp::Ge |
                    BinOp::Lt | BinOp::Le | BinOp::And | BinOp::Or => TypeCode::I64,
                    _ => if matches!(lt, TypeCode::F32 | TypeCode::F64)
                         || matches!(rt, TypeCode::F32 | TypeCode::F64)
                         { TypeCode::F64 } else { TypeCode::I64 },
                }
            }
            BoundExpr::UnaryOp(UnaryOp::Neg, inner) => inner.infer_type(schema),
            BoundExpr::UnaryOp(UnaryOp::Not, _) => TypeCode::I64,
            BoundExpr::IsNull(_) | BoundExpr::IsNotNull(_) => TypeCode::I64,
            BoundExpr::AggCall { func, arg } => match func {
                AggFunc::Avg => TypeCode::F64,
                AggFunc::Min | AggFunc::Max => {
                    if let Some(inner) = arg {
                        inner.infer_type(schema)
                    } else {
                        TypeCode::I64
                    }
                }
                _ => TypeCode::I64,
            },
        }
    }
}

#[derive(Clone, Debug, Copy)]
pub enum BinOp { Add, Sub, Mul, Div, Mod, Eq, Ne, Gt, Ge, Lt, Le, And, Or }

#[derive(Clone, Debug, Copy)]
pub enum UnaryOp { Neg, Not }
