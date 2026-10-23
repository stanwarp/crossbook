use std::fmt;
use std::str::FromStr;

use serde::de::{self, Deserialize, Deserializer};
use thiserror::Error;

const SCALE: u64 = 100_000_000;
const FRACTION_DIGITS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Px(u64);

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Qty(u64);

impl Px {
    /// The price that lies `percent` of the way from `self` to `other`.
    pub(crate) fn towards(self, other: Px, percent: u64) -> Px {
        if other.0 >= self.0 {
            Px(self.0 + (other.0 - self.0) * percent / 100)
        } else {
            Px(self.0 - (self.0 - other.0) * percent / 100)
        }
    }

    pub(crate) fn midpoint(self, other: Px) -> Px {
        Px(self.0.midpoint(other.0))
    }

    pub(crate) fn to_f64(self) -> f64 {
        scaled_to_f64(self.0)
    }
}

impl Qty {
    pub(crate) fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub(crate) fn to_f64(self) -> f64 {
        scaled_to_f64(self.0)
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ParseDecimalError {
    #[error("not a plain decimal with at most {FRACTION_DIGITS} fraction digits")]
    Malformed,
    #[error("does not fit u64 at scale 1e8")]
    Overflow,
}

fn parse_decimal(text: &str) -> Result<u64, ParseDecimalError> {
    let (integer, fraction) = text.split_once('.').unwrap_or((text, "0"));
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(integer) || !digits(fraction) || fraction.len() > FRACTION_DIGITS {
        return Err(ParseDecimalError::Malformed);
    }
    let whole: u64 = integer.parse().map_err(|_| ParseDecimalError::Overflow)?;
    let mut scaled_fraction: u64 = fraction.parse().expect("at most eight digits");
    for _ in fraction.len()..FRACTION_DIGITS {
        scaled_fraction *= 10;
    }
    whole
        .checked_mul(SCALE)
        .and_then(|value| value.checked_add(scaled_fraction))
        .ok_or(ParseDecimalError::Overflow)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "prices and sizes stay far below 2^53 at scale 1e8, where the cast is exact"
)]
fn scaled_to_f64(raw: u64) -> f64 {
    raw as f64 / 1e8
}

fn fmt_decimal(value: u64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let integer = value / SCALE;
    let mut fraction = value % SCALE;
    if fraction == 0 {
        return write!(f, "{integer}");
    }
    let mut width = FRACTION_DIGITS;
    while fraction.is_multiple_of(10) {
        fraction /= 10;
        width -= 1;
    }
    write!(f, "{integer}.{fraction:0width$}")
}

impl FromStr for Px {
    type Err = ParseDecimalError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_decimal(text).map(Px)
    }
}

impl FromStr for Qty {
    type Err = ParseDecimalError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_decimal(text).map(Qty)
    }
}

impl fmt::Display for Px {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_decimal(self.0, f)
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_decimal(self.0, f)
    }
}

impl<'de> Deserialize<'de> for Px {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <&str>::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Qty {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <&str>::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[cfg(test)]
pub(crate) fn px(text: &str) -> Px {
    text.parse().expect("test literal")
}

#[cfg(test)]
pub(crate) fn qty(text: &str) -> Qty {
    text.parse().expect("test literal")
}

#[cfg(test)]
pub(crate) fn level(price: &str, size: &str) -> (Px, Qty) {
    (px(price), qty(size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_integer_and_fraction_at_scale_1e8() {
        assert_eq!("67123.4".parse(), Ok(Px(6_712_340_000_000)));
        assert_eq!("81866.00".parse(), Ok(Px(8_186_600_000_000)));
        assert_eq!("100".parse(), Ok(Qty(10_000_000_000)));
        assert_eq!("0.00000001".parse(), Ok(Qty(1)));
        assert_eq!("0".parse(), Ok(Qty(0)));
    }

    #[test]
    fn display_trims_trailing_zeros_and_keeps_inner_ones() {
        assert_eq!(Px(8_186_610_000_000).to_string(), "81866.1");
        assert_eq!(Px(8_186_600_000_000).to_string(), "81866");
        assert_eq!(Qty(1).to_string(), "0.00000001");
        assert_eq!(Qty(100_500_000).to_string(), "1.005");
        assert_eq!(Qty(0).to_string(), "0");
    }

    #[test]
    fn rejects_anything_but_a_plain_decimal_with_at_most_eight_fraction_digits() {
        assert_eq!("1.12345678".parse(), Ok(Px(112_345_678)));
        for text in [
            "1.123456789",
            "12a.5",
            "-1",
            "+1",
            "1e5",
            "1.2.3",
            " 1",
            "",
            ".5",
            "5.",
        ] {
            assert_eq!(
                text.parse::<Px>(),
                Err(ParseDecimalError::Malformed),
                "{text:?}"
            );
        }
    }

    #[test]
    fn rejects_values_that_overflow_the_scale() {
        assert_eq!(
            "184467440738".parse::<Px>(),
            Err(ParseDecimalError::Overflow)
        );
        assert_eq!(
            "99999999999999999999999".parse::<Px>(),
            Err(ParseDecimalError::Overflow)
        );
    }

    #[test]
    fn towards_moves_a_fraction_of_the_distance_in_either_direction() {
        let low: Px = "100".parse().expect("valid literal");
        let high: Px = "200".parse().expect("valid literal");
        assert_eq!(low.towards(high, 10).to_string(), "110");
        assert_eq!(high.towards(low, 10).to_string(), "190");
        assert_eq!(low.towards(low, 10), low);
    }
}
