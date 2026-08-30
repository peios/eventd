//! Receipt-aware restart reconciliation and live sequence-gap detection.

use core::fmt;

use crate::{Coverage, Gap, Guid};

/// Per-CPU reconciliation state, owned only by that CPU's drain thread.
#[derive(Clone)]
pub struct Reconciler<'a> {
    coverage: &'a Coverage,
    boot_id: Guid,
    cpu_id: u16,
    next_sequence: u64,
    preceding_timestamp: Option<u64>,
}

impl<'a> Reconciler<'a> {
    /// Begin at sequence one for a boot stream.
    #[must_use]
    pub const fn new(coverage: &'a Coverage, boot_id: Guid, cpu_id: u16) -> Self {
        Self {
            coverage,
            boot_id,
            cpu_id,
            next_sequence: 1,
            preceding_timestamp: None,
        }
    }

    /// Reconcile one surviving ring event.
    pub fn observe(
        &mut self,
        sequence: u64,
        timestamp: u64,
        observed_at: u64,
    ) -> Result<Observation, ReconcileError> {
        if sequence == 0 {
            return Err(ReconcileError::ZeroSequence);
        }
        if sequence < self.next_sequence {
            if self.coverage.contains(&self.boot_id, self.cpu_id, sequence) {
                self.preceding_timestamp = Some(timestamp);
                return Ok(Observation {
                    store_event: false,
                    gaps: Vec::new(),
                });
            }
            return Err(ReconcileError::Regression {
                expected_at_least: self.next_sequence,
                observed: sequence,
            });
        }

        let gaps = self.uncovered_gaps(sequence, timestamp, observed_at);
        let store_event = !self.coverage.contains(&self.boot_id, self.cpu_id, sequence);
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(ReconcileError::SequenceExhausted)?;
        self.preceding_timestamp = Some(timestamp);
        Ok(Observation { store_event, gaps })
    }

    fn uncovered_gaps(&self, sequence: u64, revealing: u64, observed_at: u64) -> Vec<Gap> {
        if sequence <= self.next_sequence {
            return Vec::new();
        }
        let mut gaps = Vec::new();
        let mut first = self.next_sequence;
        let last = sequence - 1;
        let mut preceding = self.preceding_timestamp;
        for receipt in self.coverage.intervals(&self.boot_id, self.cpu_id) {
            if receipt.last < first {
                continue;
            }
            if receipt.first > last {
                break;
            }
            if first < receipt.first {
                gaps.push(make_gap(
                    first,
                    receipt.first - 1,
                    preceding,
                    revealing,
                    observed_at,
                ));
            }
            first = first.max(receipt.last.saturating_add(1));
            // A receipt proves accounting but does not tell us the preceding
            // real event's timestamp without querying event rows.
            preceding = None;
            if first > last {
                break;
            }
        }
        if first <= last {
            gaps.push(make_gap(first, last, preceding, revealing, observed_at));
        }
        gaps
    }
}

const fn make_gap(
    first_sequence: u64,
    last_sequence: u64,
    preceding_timestamp: Option<u64>,
    revealing_timestamp: u64,
    timestamp: u64,
) -> Gap {
    Gap {
        timestamp,
        first_sequence,
        last_sequence,
        preceding_timestamp,
        revealing_timestamp,
    }
}

/// Result of observing one ring event.
#[derive(Debug, PartialEq, Eq)]
pub struct Observation {
    /// Whether this event lacks a committed receipt and must be stored.
    pub store_event: bool,
    /// Missing receipt-free intervals established by this survivor.
    pub gaps: Vec<Gap>,
}

/// Incompatible KMES sequence behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileError {
    /// KMES emitted the reserved sequence zero.
    ZeroSequence,
    /// An uncovered sequence moved backwards within one boot stream.
    Regression {
        /// Smallest acceptable sequence.
        expected_at_least: u64,
        /// Sequence observed in the ring.
        observed: u64,
    },
    /// No sequence can follow `u64::MAX`.
    SequenceExhausted,
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroSequence => formatter.write_str("KMES emitted sequence zero"),
            Self::Regression {
                expected_at_least,
                observed,
            } => write!(
                formatter,
                "KMES sequence regressed: expected at least {expected_at_least}, observed {observed}"
            ),
            Self::SequenceExhausted => formatter.write_str("KMES sequence space is exhausted"),
        }
    }
}

impl std::error::Error for ReconcileError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Interval;

    #[test]
    fn restart_skips_receipts_and_gaps_only_uncovered_spans() {
        let boot = [1; 16];
        let coverage = Coverage::from_receipts([
            (boot, 2, Interval::new(1, 3).unwrap()),
            (boot, 2, Interval::new(6, 7).unwrap()),
        ]);
        let mut reconciler = Reconciler::new(&coverage, boot, 2);
        let first = reconciler.observe(3, 30, 31).unwrap();
        assert!(!first.store_event);
        assert!(first.gaps.is_empty());
        let survivor = reconciler.observe(9, 90, 91).unwrap();
        assert!(survivor.store_event);
        assert_eq!(
            survivor.gaps,
            [
                make_gap(4, 5, Some(30), 90, 91),
                make_gap(8, 8, None, 90, 91),
            ]
        );
    }

    #[test]
    fn live_jump_becomes_one_gap() {
        let coverage = Coverage::default();
        let mut reconciler = Reconciler::new(&coverage, [1; 16], 0);
        assert!(reconciler.observe(1, 10, 11).unwrap().gaps.is_empty());
        assert_eq!(
            reconciler.observe(4, 40, 41).unwrap().gaps,
            [make_gap(2, 3, Some(10), 40, 41)]
        );
    }
}
