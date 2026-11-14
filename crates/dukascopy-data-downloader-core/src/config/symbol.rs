//! Validated symbol codes.

use std::fmt;
use std::str::FromStr;

use serde::Deserialize;

use crate::AppError;

/// Shortest accepted symbol code.
const MIN_SYMBOL_LEN: usize = 3;
/// Longest accepted symbol code.
const MAX_SYMBOL_LEN: usize = 20;

/// A validated Dukascopy symbol code such as `EURUSD` or `LIGHT.CMD`.
///
/// Symbols are interpolated verbatim into both a URL path and a local
/// directory, so the character set is restricted to `[A-Z0-9.]`: anything else
/// (whitespace, `?`, `#`, `%`, separators) would either escape the intended
/// path or inject URL query/fragment delimiters. A dot is allowed because
/// Dukascopy uses it in instrument codes, but a symbol may not be `.`/`..`,
/// and may not start or end with a dot or contain consecutive dots.
///
/// The payload is an `Arc<str>`: cloning a [`crate::DownloadTask`]
/// (one per file) must not allocate a copy of the symbol for every task. The
/// type makes an invalid symbol unrepresentable rather than merely unlikely.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(std::sync::Arc<str>);

impl Symbol {
    /// Validate `value` as a symbol code and wrap it.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if the length is outside
    /// `3..=20`, if any character is outside `[A-Z0-9.]`, or if the dot is
    /// in a position that could name a special path component (leading,
    /// trailing, or repeated).
    ///
    /// # Examples
    ///
    /// ```
    /// use ddd::config::Symbol;
    ///
    /// let s = Symbol::new("EURUSD").unwrap();
    /// assert_eq!(s.as_str(), "EURUSD");
    /// assert!(Symbol::new("LIGHT.CMD").is_ok());
    /// assert!(Symbol::new("../etc").is_err());
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        let value = value.into();
        // Count *characters*, not bytes: the limit is documented as a
        // character count, and a two-character non-ASCII name is three
        // bytes long — measuring bytes would let it past a length check
        // that claims to count characters.
        let len = value.chars().count();
        if !(MIN_SYMBOL_LEN..=MAX_SYMBOL_LEN).contains(&len) {
            return Err(AppError::Config(format!(
                "symbol {value:?} must be {MIN_SYMBOL_LEN}..={MAX_SYMBOL_LEN} characters"
            )));
        }
        let bytes = value.as_bytes();
        if bytes.first() == Some(&b'.') || bytes.last() == Some(&b'.') || value.contains("..") {
            return Err(AppError::Config(format!(
                "symbol {value:?} may not start or end with a dot, or contain consecutive dots"
            )));
        }
        if !bytes
            .iter()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || *b == b'.')
        {
            return Err(AppError::Config(format!(
                "symbol {value:?} must contain only uppercase ASCII letters, digits and '.'"
            )));
        }
        Ok(Self(value.into()))
    }

    /// Borrow the underlying symbol code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Symbol {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_accepts_uppercase_alnum() {
        for s in ["EURUSD", "XAUUSD", "USDJPY", "ABC", "CRUDEOIL"] {
            assert_eq!(Symbol::new(s).unwrap().as_str(), s);
        }
    }

    /// Dukascopy uses dots in some instrument codes (`LIGHT.CMD`,
    /// `BRENT.CMD`), so a dot is allowed as an interior character.
    #[test]
    fn symbol_accepts_dotted_instruments() {
        for s in ["LIGHT.CMD", "BRENT.CMD", "EUR.USD"] {
            assert_eq!(Symbol::new(s).unwrap().as_str(), s);
        }
    }

    /// A dot must not be able to name a special path component or an empty
    /// segment. `.` and `..` are the only dangerous single components, but
    /// leading/trailing/consecutive dots are rejected as well so the rule is
    /// easy to reason about.
    #[test]
    fn symbol_rejects_unsafe_dots() {
        for bad in [".", "..", ".EURUSD", "EURUSD.", "EUR..USD", "...", ".A."] {
            let err = Symbol::new(bad).unwrap_err();
            assert!(err.to_string().contains("symbol"), "{bad}: {err}");
        }
    }

    #[test]
    fn symbol_rejects_path_traversal() {
        for bad in ["../..", "EUR/USD", "EUR\\USD", "..", "/etc"] {
            let err = Symbol::new(bad).unwrap_err();
            assert!(err.to_string().contains("symbol"), "{bad}: {err}");
        }
    }

    #[test]
    fn symbol_rejects_url_delimiters() {
        for bad in [
            "EURUSD?x=1",
            "EURUSD#f",
            "EUR USD",
            "eurusd",
            "EUR-US",
            "EUR/USD",
        ] {
            let err = Symbol::new(bad).unwrap_err();
            assert!(err.to_string().contains("symbol"), "{bad}: {err}");
        }
    }

    #[test]
    fn symbol_rejects_bad_length() {
        for bad in ["EU", "", "A23456789012345678901234"] {
            assert!(Symbol::new(bad).is_err(), "{bad} should be rejected");
        }
    }

    /// The length limit counts *characters*, not bytes.
    ///
    /// A two-character non-ASCII symbol is three bytes long, so a
    /// byte-based check would let it through a limit that is documented
    /// as 3..=20 characters.
    #[test]
    fn symbol_length_is_measured_in_characters() {
        // "欧元" is two characters but six bytes.
        let err = Symbol::new("欧元").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("3..=20 characters"), "{msg}");
        assert!(msg.contains("欧元"), "{msg}");
    }

    /// The length check runs before the charset check, so a multi-byte
    /// symbol that is *long enough* still fails on its characters.
    #[test]
    fn symbol_rejects_non_ascii_even_when_it_is_long_enough() {
        let err = Symbol::new("欧元欧元欧元").unwrap_err();
        assert!(err.to_string().contains("uppercase ASCII"), "{err}");
    }

    /// `Symbol` clones must be cheap: `DownloadTask` clones the symbol once
    /// per file, and an M1 range can contain thousands of files per pair.
    #[test]
    fn symbol_clone_does_not_copy_the_string() {
        let symbol = Symbol::new("EURUSD").unwrap();
        let clone = symbol.clone();
        assert_eq!(symbol, clone);
        assert_eq!(symbol.as_str().as_ptr(), clone.as_str().as_ptr());
    }

    #[test]
    fn symbol_accepts_boundary_lengths() {
        assert!(Symbol::new("ABC").is_ok());
        assert!(Symbol::new("ABCDEFGHIJKLMNOPQRST").is_ok()); // exactly 20
        assert!(Symbol::new("ABCDEFGHIJKLMNOPQRSTU").is_err()); // 21
    }

    #[test]
    fn symbol_display_and_parse_round_trip() {
        let s = Symbol::new("EURUSD").unwrap();
        assert_eq!(s.to_string(), "EURUSD");
        assert_eq!("EURUSD".parse::<Symbol>().unwrap(), s);
        assert!("nope".parse::<Symbol>().is_err());
    }
}
