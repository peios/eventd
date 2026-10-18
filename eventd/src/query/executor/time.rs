//! UTC calendar and relative-time interpretation for validated query syntax.

use super::QueryError;
use crate::query_language::{Query, TimeExpr};

const NANOSECONDS_PER_DAY: i64 = 86_400_000_000_000;

pub(super) fn time_range(query: &Query, evaluation: i64) -> Result<(i64, i64), QueryError> {
    let since = query
        .since
        .as_ref()
        .map_or(Ok(0), |time| resolve_time(time, evaluation))?;
    let until = query
        .until
        .as_ref()
        .map_or(Ok(evaluation), |time| resolve_time(time, evaluation))?;
    Ok((since, until))
}

fn resolve_time(time: &TimeExpr, evaluation: i64) -> Result<i64, QueryError> {
    match time {
        TimeExpr::Relative {
            nanoseconds,
            future,
        } => {
            let delta = i64::try_from(*nanoseconds).map_err(|_| QueryError::InvalidTime)?;
            if *future {
                evaluation.checked_add(delta)
            } else {
                evaluation.checked_sub(delta)
            }
            .ok_or(QueryError::InvalidTime)
        }
        TimeExpr::Today => Ok(evaluation.div_euclid(NANOSECONDS_PER_DAY) * NANOSECONDS_PER_DAY),
        TimeExpr::Yesterday => Ok(
            evaluation.div_euclid(NANOSECONDS_PER_DAY) * NANOSECONDS_PER_DAY - NANOSECONDS_PER_DAY,
        ),
        TimeExpr::Absolute(value) => parse_absolute_time(value),
    }
}

// The parser validates fixed widths and separators before constructing Absolute.
fn parse_absolute_time(value: &str) -> Result<i64, QueryError> {
    let year: i64 = value[0..4].parse().map_err(|_| QueryError::InvalidTime)?;
    let month: i64 = value[5..7].parse().map_err(|_| QueryError::InvalidTime)?;
    let day: i64 = value[8..10].parse().map_err(|_| QueryError::InvalidTime)?;
    if !(1..=12).contains(&month) || !(1..=days_in_month(year, month)).contains(&day) {
        return Err(QueryError::InvalidTime);
    }
    let (hour, minute, second) = if value.len() == 19 {
        (
            value[11..13].parse().map_err(|_| QueryError::InvalidTime)?,
            value[14..16].parse().map_err(|_| QueryError::InvalidTime)?,
            value[17..19].parse().map_err(|_| QueryError::InvalidTime)?,
        )
    } else {
        (0_i64, 0_i64, 0_i64)
    };
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return Err(QueryError::InvalidTime);
    }
    let days = days_from_civil(year, month, day);
    days.checked_mul(86_400)
        .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
        .and_then(|value| value.checked_mul(1_000_000_000))
        .ok_or(QueryError::InvalidTime)
}

const fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(mut year: i64, month: i64, day: i64) -> i64 {
    year -= i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::parse_absolute_time;

    #[test]
    fn civil_date_conversion_has_unix_epoch() {
        assert_eq!(parse_absolute_time("1970-01-01").unwrap(), 0);
        assert!(parse_absolute_time("2025-02-29").is_err());
    }

    #[test]
    fn negative_clock_components_are_rejected() {
        for value in [
            "1970-01-01T-1:00:00",
            "1970-01-01T00:-1:00",
            "1970-01-01T00:00:-1",
        ] {
            assert!(parse_absolute_time(value).is_err(), "{value}");
        }
    }
}
