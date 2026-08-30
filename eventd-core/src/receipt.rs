//! Conservative union and lookup of committed sequence receipts.

use std::collections::BTreeMap;

use crate::Guid;

/// One non-empty inclusive sequence interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    /// First covered sequence.
    pub first: u64,
    /// Last covered sequence.
    pub last: u64,
}

impl Interval {
    /// Construct a validated interval.
    #[must_use]
    pub const fn new(first: u64, last: u64) -> Option<Self> {
        if first > 0 && last >= first {
            Some(Self { first, last })
        } else {
            None
        }
    }

    /// Whether `sequence` is covered.
    #[must_use]
    pub const fn contains(self, sequence: u64) -> bool {
        sequence >= self.first && sequence <= self.last
    }
}

/// Merged committed coverage keyed by `(boot_id, logical_cpu_id)`.
#[derive(Debug, Default)]
pub struct Coverage {
    ranges: BTreeMap<(Guid, u16), Vec<Interval>>,
}

impl Coverage {
    /// Build a union from arbitrary, overlapping receipt rows.
    pub fn from_receipts(receipts: impl IntoIterator<Item = (Guid, u16, Interval)>) -> Self {
        let mut ranges: BTreeMap<(Guid, u16), Vec<Interval>> = BTreeMap::new();
        for (boot_id, cpu_id, interval) in receipts {
            ranges.entry((boot_id, cpu_id)).or_default().push(interval);
        }
        for intervals in ranges.values_mut() {
            intervals.sort_unstable_by_key(|interval| interval.first);
            let mut merged: Vec<Interval> = Vec::with_capacity(intervals.len());
            for interval in intervals.drain(..) {
                if let Some(previous) = merged.last_mut()
                    && interval.first <= previous.last.saturating_add(1)
                {
                    previous.last = previous.last.max(interval.last);
                } else {
                    merged.push(interval);
                }
            }
            *intervals = merged;
        }
        Self { ranges }
    }

    /// Whether a sequence is covered by a committed receipt.
    #[must_use]
    pub fn contains(&self, boot_id: &Guid, cpu_id: u16, sequence: u64) -> bool {
        let Some(intervals) = self.ranges.get(&(*boot_id, cpu_id)) else {
            return false;
        };
        let index = intervals.partition_point(|interval| interval.first <= sequence);
        index > 0 && intervals[index - 1].contains(sequence)
    }

    /// Borrow merged intervals for one stream.
    #[must_use]
    pub fn intervals(&self, boot_id: &Guid, cpu_id: u16) -> &[Interval] {
        self.ranges
            .get(&(*boot_id, cpu_id))
            .map_or(&[], Vec::as_slice)
    }

    /// Highest sequence covered contiguously from sequence one.
    #[must_use]
    pub fn highest_contiguous(&self, boot_id: &Guid, cpu_id: u16) -> u64 {
        self.intervals(boot_id, cpu_id)
            .first()
            .filter(|interval| interval.first == 1)
            .map_or(0, |interval| interval.last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_adjacent_and_overlapping_ranges() {
        let boot = [7; 16];
        let coverage = Coverage::from_receipts([
            (boot, 3, Interval::new(8, 12).unwrap()),
            (boot, 3, Interval::new(1, 4).unwrap()),
            (boot, 3, Interval::new(4, 7).unwrap()),
            (boot, 3, Interval::new(20, 22).unwrap()),
        ]);
        assert_eq!(
            coverage.intervals(&boot, 3),
            &[
                Interval { first: 1, last: 12 },
                Interval {
                    first: 20,
                    last: 22
                }
            ]
        );
        assert!(coverage.contains(&boot, 3, 10));
        assert!(!coverage.contains(&boot, 3, 19));
        assert_eq!(coverage.highest_contiguous(&boot, 3), 12);
    }
}
