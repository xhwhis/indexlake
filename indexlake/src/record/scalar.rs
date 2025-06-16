use std::fmt::Display;

#[derive(Debug)]
pub enum Scalar {
    Integer(Option<i32>),
    BigInt(Option<i64>),
    Float(Option<f32>),
    Double(Option<f64>),
    Varchar(Option<String>),
    Varbinary(Option<Vec<u8>>),
    Boolean(Option<bool>),
}

impl Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Integer(Some(value)) => write!(f, "{}", value),
            Scalar::Integer(None) => write!(f, "null"),
            Scalar::BigInt(Some(value)) => write!(f, "{}", value),
            Scalar::BigInt(None) => write!(f, "null"),
            Scalar::Float(Some(value)) => write!(f, "{}", value),
            Scalar::Float(None) => write!(f, "null"),
            Scalar::Double(Some(value)) => write!(f, "{}", value),
            Scalar::Double(None) => write!(f, "null"),
            Scalar::Varchar(Some(value)) => write!(f, "'{}'", value),
            Scalar::Varchar(None) => write!(f, "null"),
            Scalar::Varbinary(Some(value)) => write!(f, "X'{}'", hex::encode(value)),
            Scalar::Varbinary(None) => write!(f, "null"),
            Scalar::Boolean(Some(value)) => write!(f, "{}", value),
            Scalar::Boolean(None) => write!(f, "null"),
        }
    }
}
