use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A wrapper for data type values that correspond to PostgreSQL enum values.
/// Handles any enum values dynamically without requiring code changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataType(pub String);

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DataType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(DataType(s.to_string()))
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        DataType(s)
    }
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        DataType(s.to_string())
    }
}
