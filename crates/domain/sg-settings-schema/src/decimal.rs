//! Exact decimal comparison used by the JSON-Schema `minimum`/`maximum`
//! checks (gojsonschema uses `big.Rat`; `f64` would lose precision on the
//! long numbers settings can contain).
//!
//! A value is `sign * digits * 10^exp10`, with `digits` normalized to have
//! no leading or trailing zeros (`"0"` for zero).

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    negative: bool,
    digits: String,
    exp10: i64,
}

impl Decimal {
    /// Parses a JSON number lexeme. Returns `None` if it is malformed
    /// (the tolerant parser may still produce such lexemes when it reports
    /// an error, in which case callers never validate them).
    pub fn parse(text: &str) -> Option<Self> {
        let (negative, rest) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let (mantissa, exponent) = match rest.find(['e', 'E']) {
            Some(index) => {
                let exponent: i64 = rest[index + 1..].parse().ok()?;
                (&rest[..index], exponent)
            }
            None => (rest, 0),
        };
        let (integer, fraction) = match mantissa.split_once('.') {
            Some((integer, fraction)) => (integer, fraction),
            None => (mantissa, ""),
        };
        if integer.is_empty() || !integer.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }

        let mut digits: String = format!("{integer}{fraction}");
        let exp10 = exponent.checked_sub(i64::try_from(fraction.len()).ok()?)?;

        let trimmed = digits.trim_start_matches('0');
        if trimmed.is_empty() {
            return Some(Self {
                negative: false,
                digits: "0".to_owned(),
                exp10: 0,
            });
        }
        digits = trimmed.to_owned();
        let trailing_zeros = digits
            .bytes()
            .rev()
            .take_while(|&byte| byte == b'0')
            .count();
        if trailing_zeros > 0 {
            digits.truncate(digits.len() - trailing_zeros);
        }
        Some(Self {
            negative,
            digits,
            exp10: exp10.checked_add(i64::try_from(trailing_zeros).ok()?)?,
        })
    }

    /// `big.Rat.IsInt`: the value has no fractional part.
    pub fn is_integer(&self) -> bool {
        self.is_zero() || self.exp10 >= 0
    }

    fn is_zero(&self) -> bool {
        self.digits == "0"
    }

    /// `Cmp` against another decimal.
    pub fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (self.is_zero(), other.is_zero()) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if other.negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, true) => {
                return if self.negative {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            _ => {}
        }
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let magnitude = self.magnitude_cmp(other);
        if self.negative {
            magnitude.reverse()
        } else {
            magnitude
        }
    }

    /// Compares absolute values.
    fn magnitude_cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let self_digits = i64::try_from(self.digits.len()).unwrap_or(i64::MAX);
        let other_digits = i64::try_from(other.digits.len()).unwrap_or(i64::MAX);
        let self_size = self_digits.saturating_add(self.exp10);
        let other_size = other_digits.saturating_add(other.exp10);
        match self_size.cmp(&other_size) {
            Ordering::Equal => {}
            other => return other,
        }
        let width = self.digits.len().max(other.digits.len());
        let self_padded = format!("{:0<width$}", self.digits);
        let other_padded = format!("{:0<width$}", other.digits);
        self_padded.cmp(&other_padded)
    }
}

#[cfg(test)]
mod tests {
    use super::Decimal;
    use std::cmp::Ordering;

    fn parse(text: &str) -> Decimal {
        Decimal::parse(text).expect("valid number")
    }

    #[test]
    fn detects_integers() {
        assert!(parse("0").is_integer());
        assert!(parse("-0").is_integer());
        assert!(parse("1").is_integer());
        assert!(parse("1.0").is_integer());
        assert!(!parse("1.50").is_integer());
        assert!(parse("2.00").is_integer());
        assert!(parse("1e3").is_integer());
        assert!(parse("1.5e1").is_integer());
        assert!(!parse("1.5").is_integer());
        assert!(!parse("1e-1").is_integer());
        assert!(!parse("0.0001").is_integer());
    }

    #[test]
    fn compares() {
        assert_eq!(parse("1").cmp(&parse("2")), Ordering::Less);
        assert_eq!(parse("10").cmp(&parse("9.999")), Ordering::Greater);
        assert_eq!(parse("-2").cmp(&parse("-1")), Ordering::Less);
        assert_eq!(parse("1.0").cmp(&parse("1")), Ordering::Equal);
        assert_eq!(parse("1e2").cmp(&parse("100")), Ordering::Equal);
        assert_eq!(parse("0.1").cmp(&parse("0.09")), Ordering::Greater);
        assert_eq!(parse("-0.1").cmp(&parse("0.09")), Ordering::Less);
    }
}
