use crate::core::{DbError, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

impl Value {
    /// Парсинг числа БЕЗ аллокаций
    #[inline]
    pub fn parse_number(s: &str) -> Result<Self> {
        // Быстрая проверка на integer (без точки и экспоненты)
        let has_dot_or_exp = s.bytes().any(|b| b == b'.' || b == b'e' || b == b'E');

        if !has_dot_or_exp
            && let Ok(i) = s.parse::<i64>() {
                return Ok(Value::Integer(i));
            }

        // Float
        if let Ok(f) = s.parse::<f64>() {
            Ok(Value::Float(f))
        } else {
            Err(DbError::TypeMismatch(format!("Invalid number: {}", s)))
        }
    }
}

impl Value {
    pub fn compare(&self, other: &Value) -> Result<Ordering> {
        match (self, other) {
            // ========================================
            // NULL handling: NULL is "greater" than all values (NULL LAST)
            // ========================================
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Null, _) => Ok(Ordering::Greater),
            (_, Value::Null) => Ok(Ordering::Less),

            // ========================================
            // Same type comparisons
            // ========================================
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),

            (Value::Float(a), Value::Float(b)) => {
                // Handle NaN: NaN is considered equal to NaN, greater than all other values
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ok(Ordering::Equal),
                    (true, false) => Ok(Ordering::Greater),
                    (false, true) => Ok(Ordering::Less),
                    (false, false) => Ok(a.partial_cmp(b).unwrap_or(Ordering::Equal)),
                }
            }

            (Value::Text(a), Value::Text(b)) => Ok(a.cmp(b)),

            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),

            // ========================================
            // Mixed numeric types (implicit coercion)
            // ========================================
            (Value::Integer(a), Value::Float(b)) => {
                let a_float = *a as f64;
                match (a_float.is_nan(), b.is_nan()) {
                    (true, true) => Ok(Ordering::Equal),
                    (true, false) => Ok(Ordering::Greater),
                    (false, true) => Ok(Ordering::Less),
                    (false, false) => Ok(a_float.partial_cmp(b).unwrap_or(Ordering::Equal)),
                }
            }

            (Value::Float(a), Value::Integer(b)) => {
                let b_float = *b as f64;
                match (a.is_nan(), b_float.is_nan()) {
                    (true, true) => Ok(Ordering::Equal),
                    (true, false) => Ok(Ordering::Greater),
                    (false, true) => Ok(Ordering::Less),
                    (false, false) => Ok(a.partial_cmp(&b_float).unwrap_or(Ordering::Equal)),
                }
            }

            // ========================================
            // Type mismatches - ERROR
            // ========================================
            _ => Err(DbError::TypeMismatch(format!(
                "Cannot compare incompatible types: {} and {}",
                self.type_name(),
                other.type_name()
            ))),
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Integer(_) => "INTEGER",
            Self::Float(_) => "FLOAT",
            Self::Text(_) => "TEXT",
            Self::Boolean(_) => "BOOLEAN",
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Boolean(b) => *b,
            Self::Integer(i) => *i != 0,
            Self::Float(f) => *f != 0.0 && !f.is_nan(),
            Self::Text(s) => !s.is_empty(),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            Self::Float(f) => {
                if f.is_finite() && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                    Some(*f as i64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer(_) | Self::Float(_))
    }

    fn type_index(&self) -> u8 {
        match self {
            Self::Null => 0,
            Self::Integer(_) => 1,
            Self::Float(_) => 2,
            Self::Text(_) => 3,
            Self::Boolean(_) => 4,
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::Float(a), Self::Float(b)) => {
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => false, // NaN != NaN по стандарту
                    (true, false) | (false, true) => false,
                    _ => {
                        // Относительное сравнение для больших чисел
                        let diff = (a - b).abs();
                        let largest = a.abs().max(b.abs());
                        diff <= largest * f64::EPSILON * 8.0
                    }
                }
            }
            (Self::Text(a), Self::Text(b)) => a == b,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            // Автоматическое преобразование между Integer и Float
            (Self::Integer(i), Self::Float(f)) | (Self::Float(f), Self::Integer(i)) => {
                (*i as f64 - f).abs() < f64::EPSILON
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, Self::Null) => Some(Ordering::Equal),
            (Self::Null, _) => Some(Ordering::Less),
            (_, Self::Null) => Some(Ordering::Greater),

            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::Float(a), Self::Float(b)) => a.partial_cmp(b),
            (Self::Integer(i), Self::Float(f)) => (*i as f64).partial_cmp(f),
            (Self::Float(f), Self::Integer(i)) => f.partial_cmp(&(*i as f64)),

            (Self::Text(a), Self::Text(b)) => a.partial_cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.partial_cmp(b),

            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Null, _) => Ordering::Less,
            (_, Self::Null) => Ordering::Greater,

            (Self::Integer(a), Self::Integer(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => {
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => a.partial_cmp(b).unwrap(),
                }
            }
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),

            // Cross-type comparison: order by type variant index
            // Null (0) < Integer (1) < Float (2) < Text (3) < Boolean (4)
            (a, b) => {
                let a_idx = a.type_index();
                let b_idx = b.type_index();
                a_idx.cmp(&b_idx)
            }
        }
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Null => 0u8.hash(state),
            Self::Integer(i) => {
                1u8.hash(state);
                i.hash(state);
            }
            Self::Float(f) => {
                2u8.hash(state);
                f.to_bits().hash(state);
            }
            Self::Text(s) => {
                3u8.hash(state);
                s.hash(state);
            }
            Self::Boolean(b) => {
                4u8.hash(state);
                b.hash(state);
            }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(fl) => {
                if fl.is_nan() {
                    write!(f, "NaN")
                } else if fl.is_infinite() {
                    if *fl > 0.0 {
                        write!(f, "Infinity")
                    } else {
                        write!(f, "-Infinity")
                    }
                } else {
                    write!(f, "{}", fl)
                }
            }
            Self::Text(s) => write!(f, "{}", s),
            Self::Boolean(b) => write!(f, "{}", b),
        }
    }
}

// Реализация From для удобного создания значений
impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
}

impl DataType {
    pub fn is_compatible(&self, value: &Value) -> bool {
        match (self, value) {
            (_, Value::Null) => true,
            (Self::Integer, Value::Integer(_)) => true,
            (Self::Float, Value::Float(_)) => true,
            (Self::Float, Value::Integer(_)) => true, // Разрешаем Integer -> Float
            (Self::Text, Value::Text(_)) => true,
            (Self::Boolean, Value::Boolean(_)) => true,
            _ => false,
        }
    }

    pub fn can_cast_to(&self, other: &DataType) -> bool {
        match (self, other) {
            (a, b) if a == b => true,
            (Self::Integer, Self::Float) => true,
            (Self::Integer, Self::Text) => true,
            (Self::Float, Self::Text) => true,
            (Self::Boolean, Self::Text) => true,
            _ => false,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "FLOAT"),
            Self::Text => write!(f, "TEXT"),
            Self::Boolean => write!(f, "BOOLEAN"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Integer(42), Value::Integer(42));
        assert_eq!(Value::Float(3.14), Value::Float(3.14));
        assert_ne!(Value::Integer(1), Value::Integer(2));
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Text("a".into()) < Value::Text("b".into()));
        assert!(Value::Null < Value::Integer(0));
    }

    #[test]
    fn test_type_compatibility() {
        let int_type = DataType::Integer;
        assert!(int_type.is_compatible(&Value::Integer(42)));
        assert!(int_type.is_compatible(&Value::Null));
        assert!(!int_type.is_compatible(&Value::Text("hello".into())));
    }
}