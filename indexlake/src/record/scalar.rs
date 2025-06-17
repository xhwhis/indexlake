use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum Scalar {
    Integer(Option<i32>),
    BigInt(Option<i64>),
    Float(Option<f32>),
    Double(Option<f64>),
    Varchar(Option<String>),
    Varbinary(Option<Vec<u8>>),
    Boolean(Option<bool>),
}

impl Scalar {
    pub fn is_null(&self) -> bool {
        match self {
            Scalar::Integer(None) => true,
            Scalar::Integer(Some(_)) => false,
            Scalar::BigInt(None) => true,
            Scalar::BigInt(Some(_)) => false,
            Scalar::Float(None) => true,
            Scalar::Float(Some(_)) => false,
            Scalar::Double(None) => true,
            Scalar::Double(Some(_)) => false,
            Scalar::Varchar(None) => true,
            Scalar::Varchar(Some(_)) => false,
            Scalar::Varbinary(None) => true,
            Scalar::Varbinary(Some(_)) => false,
            Scalar::Boolean(None) => true,
            Scalar::Boolean(Some(_)) => false,
        }
    }

    pub fn is_true(&self) -> bool {
        match self {
            Scalar::Boolean(Some(true)) => true,
            _ => false,
        }
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Scalar::Integer(v1), Scalar::Integer(v2)) => v1.eq(v2),
            (Scalar::Integer(_), _) => false,
            (Scalar::BigInt(v1), Scalar::BigInt(v2)) => v1.eq(v2),
            (Scalar::BigInt(_), _) => false,
            (Scalar::Float(v1), Scalar::Float(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Scalar::Float(_), _) => false,
            (Scalar::Double(v1), Scalar::Double(v2)) => match (v1, v2) {
                (Some(d1), Some(d2)) => d1.to_bits() == d2.to_bits(),
                _ => v1.eq(v2),
            },
            (Scalar::Double(_), _) => false,
            (Scalar::Varchar(v1), Scalar::Varchar(v2)) => v1.eq(v2),
            (Scalar::Varchar(_), _) => false,
            (Scalar::Varbinary(v1), Scalar::Varbinary(v2)) => v1.eq(v2),
            (Scalar::Varbinary(_), _) => false,
            (Scalar::Boolean(v1), Scalar::Boolean(v2)) => v1.eq(v2),
            (Scalar::Boolean(_), _) => false,
        }
    }
}

impl Eq for Scalar {}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Scalar::Integer(v1), Scalar::Integer(v2)) => v1.partial_cmp(v2),
            (Scalar::Integer(_), _) => None,
            (Scalar::BigInt(v1), Scalar::BigInt(v2)) => v1.partial_cmp(v2),
            (Scalar::BigInt(_), _) => None,
            (Scalar::Float(v1), Scalar::Float(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.partial_cmp(f2),
                _ => v1.partial_cmp(v2),
            },
            (Scalar::Float(_), _) => None,
            (Scalar::Double(v1), Scalar::Double(v2)) => match (v1, v2) {
                (Some(d1), Some(d2)) => d1.partial_cmp(d2),
                _ => v1.partial_cmp(v2),
            },
            (Scalar::Double(_), _) => None,
            (Scalar::Varchar(v1), Scalar::Varchar(v2)) => v1.partial_cmp(v2),
            (Scalar::Varchar(_), _) => None,
            (Scalar::Varbinary(v1), Scalar::Varbinary(v2)) => v1.partial_cmp(v2),
            (Scalar::Varbinary(_), _) => None,
            (Scalar::Boolean(v1), Scalar::Boolean(v2)) => v1.partial_cmp(v2),
            (Scalar::Boolean(_), _) => None,
        }
    }
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
