//! Canonical candle timeframes.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

use crate::AppError;

/// Canonical timeframe. Aliases (`1m`/`1h`/`1d`) parse to the same variant,
/// so `["M1", "1m"]` dedups instead of downloading twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Timeframe {
    /// One-minute bars (server file stem `candles_min_1`).
    M1,
    /// One-hour bars (server file stem `candles_hour_1`).
    H1,
    /// One-day bars (server file stem `candles_day_1`).
    D1,
}

impl FromStr for Timeframe {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "M1" | "1m" => Ok(Self::M1),
            "H1" | "1h" => Ok(Self::H1),
            "D1" | "1d" => Ok(Self::D1),
            _ => Err(AppError::Config(format!(
                "unsupported timeframe: {s}, valid values: [\"M1\", \"H1\", \"D1\", \"1m\", \"1h\", \"1d\"]"
            ))),
        }
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::M1 => write!(f, "M1"),
            Self::H1 => write!(f, "H1"),
            Self::D1 => write!(f, "D1"),
        }
    }
}

impl<'de> Deserialize<'de> for Timeframe {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeframe_display_round_trips() {
        for tf in [Timeframe::M1, Timeframe::H1, Timeframe::D1] {
            assert_eq!(tf.to_string().parse::<Timeframe>().unwrap(), tf);
        }
    }
}
