use std::sync::LazyLock;

use regex::Regex;

/// The conversion that parses with the format. Only `Timestamp` admits time elements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FormatTarget {
    Date,
    Timestamp,
}

// The one place naming supported parts and separators. `-u` keeps `ſ` from matching `S`.
static PARTS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i-u)\.?FF[369]|HH24|YYYY|MM|MI|DD|SS|[-/.,;:_ ]").unwrap());

/// Translates an Exasol format model to a strftime pattern. `None` means no exact strftime
/// equivalent exists, so the caller declines.
pub(crate) fn strftime_pattern(format: &str, target: FormatTarget) -> Option<String> {
    let mut pattern = String::new();
    let mut end = 0;
    let mut counts = PartCounts::default();
    for part in PARTS.find_iter(format) {
        if part.start() != end {
            return None;
        }
        end = part.end();
        let part = part.as_str().to_ascii_uppercase();
        pattern.push_str(match part.as_str() {
            ".FF3" => "%.3f",
            ".FF6" => "%.6f",
            ".FF9" => "%.9f",
            "FF3" => "%3f",
            "FF6" => "%6f",
            "FF9" => "%9f",
            "HH24" => "%H",
            "YYYY" => "%Y",
            "MM" => "%m",
            "MI" => "%M",
            "DD" => "%d",
            "SS" => "%S",
            separator => separator,
        });
        counts.add(&part);
    }
    (end == format.len() && counts.is_supported(target)).then_some(pattern)
}

#[derive(Default)]
struct PartCounts {
    year: usize,
    month: usize,
    day: usize,
    hour: usize,
    minute: usize,
    second: usize,
    fraction: usize,
}

impl PartCounts {
    fn add(&mut self, part: &str) {
        match part {
            "YYYY" => self.year += 1,
            "MM" => self.month += 1,
            "DD" => self.day += 1,
            "HH24" => self.hour += 1,
            "MI" => self.minute += 1,
            "SS" => self.second += 1,
            _ if part.contains("FF") => self.fraction += 1,
            _ => {}
        }
    }

    fn is_supported(&self, target: FormatTarget) -> bool {
        let date_parts = (self.year, self.month, self.day) == (1, 1, 1);
        let time_parts = (self.hour, self.minute, self.second, self.fraction);
        let time_supported = match target {
            FormatTarget::Date => time_parts == (0, 0, 0, 0),
            FormatTarget::Timestamp => {
                matches!(time_parts, (0, 0, 0, 0) | (1, 1, 0, 0) | (1, 1, 1, 0 | 1))
            }
        };
        date_parts && time_supported
    }
}

#[cfg(test)]
#[path = "datetime_format_tests.rs"]
mod tests;
