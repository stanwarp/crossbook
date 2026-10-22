use std::fmt;
use std::str::FromStr;

use serde::de::{self, Deserialize, Deserializer, Visitor};
use thiserror::Error;

const SCALE: u64 = 100_000_000;
const FRACTION_DIGITS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct Px(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) struct Qty(u64);

impl Px {
    /// The price that lies `percent` of the way from `self` to `other`.
    pub(crate) fn towards(self, other: Px, percent: u64) -> Px {
        if other.0 >= self.0 {
            Px(self.0 + (other.0 - self.0) * percent / 100)
        } else {
            Px(self.0 - (self.0 - other.0) * percent / 100)
        }
    }
}

impl Qty {
    pub(crate) fn is_zero(self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub(crate) enum ParseDecimalError {
    #[error("no digits before the decimal point")]
    MissingIntegerDigits,
    #[error("no digits after the decimal point")]
    MissingFractionDigits,
    #[error("more than {FRACTION_DIGITS} fraction digits")]
    TooManyFractionDigits,
    #[error("unexpected character {0:?}")]
    NonDigit(char),
    #[error("value does not fit u64 at scale 1e8")]
    Overflow,
}

fn parse_scaled(text: &str) -> Result<u64, ParseDecimalError> {
    let (integer, fraction) = match text.split_once('.') {
        Some((integer, fraction)) => (integer, Some(fraction)),
        None => (text, None),
    };
    if integer.is_empty() {
        return Err(ParseDecimalError::MissingIntegerDigits);
    }
    let mut value: u64 = 0;
    for c in integer.chars() {
        let digit =
            c.to_digit(10).ok_or(ParseDecimalError::NonDigit(c))?;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(digit)))
            .ok_or(ParseDecimalError::Overflow)?;
    }
    value = value
        .checked_mul(SCALE)
        .ok_or(ParseDecimalError::Overflow)?;
    let Some(fraction) = fraction else {
        return Ok(value);
    };
    if fraction.is_empty() {
        return Err(ParseDecimalError::MissingFractionDigits);
    }
    let mut place = SCALE;
    for (index, c) in fraction.chars().enumerate() {
        if index >= FRACTION_DIGITS {
            return Err(ParseDecimalError::TooManyFractionDigits);
        }
        let digit =
            c.to_digit(10).ok_or(ParseDecimalError::NonDigit(c))?;
        place /= 10;
        value = value
            .checked_add(u64::from(digit) * place)
            .ok_or(ParseDecimalError::Overflow)?;
    }
    Ok(value)
}

fn fmt_scaled(value: u64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        parse_scaled(text).map(Px)
    }
}

impl FromStr for Qty {
    type Err = ParseDecimalError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_scaled(text).map(Qty)
    }
}

impl fmt::Display for Px {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_scaled(self.0, f)
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_scaled(self.0, f)
    }
}

struct ScaledVisitor;

impl Visitor<'_> for ScaledVisitor {
    type Value = u64;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a decimal string with at most 8 fraction digits")
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<u64, E> {
        parse_scaled(text).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for Px {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        deserializer.deserialize_str(ScaledVisitor).map(Px)
    }
}

impl<'de> Deserialize<'de> for Qty {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        deserializer.deserialize_str(ScaledVisitor).map(Qty)
    }
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
    fn rejects_more_than_eight_fraction_digits() {
        assert_eq!(
            "1.123456789".parse::<Px>(),
            Err(ParseDecimalError::TooManyFractionDigits)
        );
        assert_eq!("1.12345678".parse(), Ok(Px(112_345_678)));
    }

    #[test]
    fn rejects_non_digit_characters() {
        assert_eq!(
            "12a.5".parse::<Px>(),
            Err(ParseDecimalError::NonDigit('a'))
        );
        assert_eq!(
            "-1".parse::<Px>(),
            Err(ParseDecimalError::NonDigit('-'))
        );
        assert_eq!(
            "1e5".parse::<Qty>(),
            Err(ParseDecimalError::NonDigit('e'))
        );
        assert_eq!(
            "1.2.3".parse::<Qty>(),
            Err(ParseDecimalError::NonDigit('.'))
        );
        assert_eq!(
            " 1".parse::<Qty>(),
            Err(ParseDecimalError::NonDigit(' '))
        );
    }

    #[test]
    fn rejects_missing_digits_around_the_point() {
        assert_eq!(
            "".parse::<Px>(),
            Err(ParseDecimalError::MissingIntegerDigits)
        );
        assert_eq!(
            ".5".parse::<Px>(),
            Err(ParseDecimalError::MissingIntegerDigits)
        );
        assert_eq!(
            "5.".parse::<Px>(),
            Err(ParseDecimalError::MissingFractionDigits)
        );
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
    fn towards_moves_a_fraction_of_the_distance_in_either_direction()
    {
        let low: Px = "100".parse().expect("valid literal");
        let high: Px = "200".parse().expect("valid literal");
        assert_eq!(low.towards(high, 10).to_string(), "110");
        assert_eq!(high.towards(low, 10).to_string(), "190");
        assert_eq!(low.towards(low, 10), low);
    }
}
