//! Writing values into query text (PSPU §3.19), so that what a person
//! typed is always one value and never more query.
//!
//! Every place the language takes an identifier or a pattern — a field, an
//! event type pattern, a log origin, `CONTAINING`'s text — also takes a
//! quoted string, so [`string`] is always safe there. It is also the only
//! safe choice for text a person typed: an origin called `STREAM` or
//! `WHERE` written bare would be read as a clause.

use core::fmt::Write as _;
use std::time::Duration;

/// `value` as a quoted string literal.
///
/// The quote and backslash are escaped, and so is every control
/// character: `\n`, `\r` and `\t` by name, the rest as `\uXXXX`. Every
/// other character is written as itself, outside the basic multilingual
/// plane included, which is the one way §3.19 allows to write those.
#[must_use]
pub fn string(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            control if control.is_control() && u32::from(control) <= 0xffff => {
                write!(quoted, "\\u{:04X}", u32::from(control)).expect("writing to a String");
            }
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

/// `bytes` as a binary literal, `x"…"`, which compares only against binary
/// values (§3.19).
#[must_use]
pub fn binary(bytes: &[u8]) -> String {
    let mut literal = String::with_capacity(bytes.len() * 2 + 3);
    literal.push_str("x\"");
    for byte in bytes {
        write!(literal, "{byte:02x}").expect("writing to a String");
    }
    literal.push('"');
    literal
}

/// Whether `value` may be written bare as an identifier.
///
/// The grammar is `[A-Za-z_][A-Za-z0-9_.-]*`. Bare or quoted, it means the
/// same, but a bare word that is also a clause keyword is read as the
/// keyword; for anything a person typed, use [`string`].
#[must_use]
pub fn is_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

/// `duration` as a duration literal, in the largest unit that holds it.
///
/// `90s` stays `90s` and `7200s` becomes `2h`. It is `None` when the
/// duration is zero or not a whole number of seconds, which the language
/// cannot write.
#[must_use]
pub fn duration(duration: Duration) -> Option<String> {
    if duration.is_zero() || duration.subsec_nanos() != 0 {
        return None;
    }
    let seconds = duration.as_secs();
    let (count, unit) = [(86_400, 'd'), (3_600, 'h'), (60, 'm')]
        .into_iter()
        .find(|(size, _)| seconds.is_multiple_of(*size))
        .map_or((seconds, 's'), |(size, unit)| (seconds / size, unit));
    Some(format!("{count}{unit}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_escape_quotes_backslashes_and_controls() {
        assert_eq!(string("plain"), "\"plain\"");
        assert_eq!(string("say \"hi\" \\ bye"), "\"say \\\"hi\\\" \\\\ bye\"");
        assert_eq!(string("a\nb\rc\td"), "\"a\\nb\\rc\\td\"");
        assert_eq!(string("\u{0}\u{1b}\u{7f}"), "\"\\u0000\\u001B\\u007F\"");
        assert_eq!(string("é 😀"), "\"é 😀\"");
    }

    #[test]
    fn binary_is_lowercase_hex() {
        assert_eq!(binary(&[0x01, 0xAB]), "x\"01ab\"");
        assert_eq!(binary(&[]), "x\"\"");
    }

    #[test]
    fn identifiers_follow_the_grammar() {
        assert!(is_identifier("kacs.access_denied"));
        assert!(is_identifier("_a-b.c9"));
        assert!(!is_identifier("9lives"));
        assert!(!is_identifier("jobs/x"));
        assert!(!is_identifier("kacs.*"));
        assert!(!is_identifier(""));
    }

    #[test]
    fn durations_use_the_largest_exact_unit() {
        assert_eq!(duration(Duration::from_secs(90)).as_deref(), Some("90s"));
        assert_eq!(duration(Duration::from_secs(120)).as_deref(), Some("2m"));
        assert_eq!(duration(Duration::from_secs(7_200)).as_deref(), Some("2h"));
        let two_days = 2 * 86_400;
        assert_eq!(
            duration(Duration::from_secs(two_days)).as_deref(),
            Some("2d")
        );
        assert_eq!(duration(Duration::ZERO), None);
        assert_eq!(duration(Duration::from_millis(1_500)), None);
    }
}
