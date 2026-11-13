//! Bid/ask price types.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

use crate::AppError;

/// Canonical price type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum PriceType {
    /// Bid/ASK candles (`BID_*` server file prefix). Default.
    #[default]
    Bid,
    /// Ask candles (`ASK_*` server file prefix).
    Ask,
}

impl FromStr for PriceType {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BID" => Ok(Self::Bid),
            "ASK" => Ok(Self::Ask),
            _ => Err(AppError::Config(format!(
                "unsupported price type: {s}, valid values: [\"BID\", \"ASK\"]"
            ))),
        }
    }
}

impl fmt::Display for PriceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bid => write!(f, "BID"),
            Self::Ask => write!(f, "ASK"),
        }
    }
}

impl<'de> Deserialize<'de> for PriceType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_type_display_round_trips() {
        for pt in [PriceType::Bid, PriceType::Ask] {
            assert_eq!(pt.to_string().parse::<PriceType>().unwrap(), pt);
        }
    }
}
