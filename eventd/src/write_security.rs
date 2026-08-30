//! KACS-backed metric publication checks with a bounded thread-local cache.

use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use eventd_core::MetricRecord;
use peios::token::Token;

use crate::query::DescriptorCache;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TokenFingerprint {
    token_id: u64,
    modified_id: u64,
}

/// The metric ingestion thread's publication authorizer.
///
/// The outer map separates stable captured producer tokens. The inner map is
/// borrowed by `&str` on a hit, so recurring metric names allocate nothing.
/// When the global bound is reached the whole cache is discarded: this is
/// cheaper than maintaining an LRU on the write path, remains strictly
/// bounded, and only penalises a workload already churning unique principals
/// or names.
pub struct MetricPublishAuthorizer {
    descriptors: Arc<DescriptorCache>,
    generation: u64,
    capacity: usize,
    entries: usize,
    verdicts: HashMap<TokenFingerprint, HashMap<Box<str>, bool>>,
}

impl MetricPublishAuthorizer {
    pub fn new(descriptors: Arc<DescriptorCache>, capacity: usize) -> Self {
        let generation = descriptors.generation();
        Self {
            descriptors,
            generation,
            capacity,
            entries: 0,
            verdicts: HashMap::new(),
        }
    }

    pub fn configure(&mut self, capacity: usize) {
        self.capacity = capacity;
        if self.entries > capacity {
            self.clear();
        }
    }

    /// Remove records whose names the conveyed producer token cannot publish.
    /// Returns the number rejected. A policy or token-query error leaves the
    /// caller responsible for dropping the entire datagram fail-closed.
    pub fn authorize(
        &mut self,
        token: &Token,
        records: &mut Vec<MetricRecord>,
    ) -> Result<usize, MetricPublishError> {
        self.sync_generation();
        let statistics = token.statistics().map_err(MetricPublishError::Token)?;
        let fingerprint = TokenFingerprint {
            token_id: statistics.token_id,
            modified_id: statistics.modified_id,
        };
        let before = records.len();
        let mut failure = None;
        records.retain(|record| {
            if failure.is_some() {
                return false;
            }
            match self.authorize_name(token, fingerprint, &record.name) {
                Ok(allowed) => allowed,
                Err(error) => {
                    failure = Some(error);
                    false
                }
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(before - records.len())
    }

    fn authorize_name(
        &mut self,
        token: &Token,
        fingerprint: TokenFingerprint,
        name: &str,
    ) -> Result<bool, MetricPublishError> {
        let descriptors = Arc::clone(&self.descriptors);
        self.authorize_name_with(fingerprint, name, || {
            descriptors
                .check_metric_publish(token, name)
                .map_err(|error| MetricPublishError::Policy(error.to_string()))
        })
    }

    fn authorize_name_with<E>(
        &mut self,
        fingerprint: TokenFingerprint,
        name: &str,
        check: impl FnOnce() -> Result<(u64, bool), E>,
    ) -> Result<bool, E> {
        if let Some(verdict) = self
            .verdicts
            .get(&fingerprint)
            .and_then(|names| names.get(name))
        {
            return Ok(*verdict);
        }
        let (generation, verdict) = check()?;
        if generation != self.generation {
            self.generation = generation;
            self.clear();
        }
        self.insert(fingerprint, name, verdict);
        Ok(verdict)
    }

    fn sync_generation(&mut self) {
        let generation = self.descriptors.generation();
        self.sync_generation_to(generation);
    }

    fn sync_generation_to(&mut self, generation: u64) {
        if generation != self.generation {
            self.generation = generation;
            self.clear();
        }
    }

    fn insert(&mut self, fingerprint: TokenFingerprint, name: &str, verdict: bool) {
        if self.capacity == 0 {
            return;
        }
        if self.entries == self.capacity {
            self.clear();
        }
        self.verdicts
            .entry(fingerprint)
            .or_default()
            .insert(name.into(), verdict);
        self.entries += 1;
    }

    fn clear(&mut self) {
        self.verdicts.clear();
        self.entries = 0;
    }
}

#[derive(Debug)]
pub enum MetricPublishError {
    Token(peios::Error),
    Policy(String),
}

impl fmt::Display for MetricPublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token(error) => {
                write!(formatter, "cannot inspect metric producer token: {error}")
            }
            Self::Policy(error) => {
                write!(formatter, "cannot authorize metric publication: {error}")
            }
        }
    }
}

impl std::error::Error for MetricPublishError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn authorizer(capacity: usize) -> MetricPublishAuthorizer {
        MetricPublishAuthorizer::new(Arc::new(DescriptorCache::new()), capacity)
    }

    #[test]
    fn verdict_cache_is_bounded_without_lru_bookkeeping() {
        let mut authorizer = authorizer(2);
        let token = TokenFingerprint {
            token_id: 1,
            modified_id: 2,
        };
        authorizer.insert(token, "one", true);
        authorizer.insert(token, "two", false);
        assert_eq!(authorizer.entries, 2);
        authorizer.insert(token, "three", true);
        assert_eq!(authorizer.entries, 1);
        assert_eq!(authorizer.verdicts[&token].get("three"), Some(&true));
        assert!(!authorizer.verdicts[&token].contains_key("one"));
    }

    #[test]
    fn descriptor_generation_change_discards_every_verdict() {
        let mut authorizer = authorizer(8);
        let token = TokenFingerprint {
            token_id: 1,
            modified_id: 2,
        };
        authorizer.insert(token, "one", true);
        authorizer.sync_generation_to(authorizer.generation.wrapping_add(1));
        assert_eq!(authorizer.entries, 0);
        assert!(authorizer.verdicts.is_empty());
    }

    #[test]
    fn zero_capacity_disables_caching() {
        let mut authorizer = authorizer(0);
        authorizer.insert(
            TokenFingerprint {
                token_id: 1,
                modified_id: 2,
            },
            "one",
            true,
        );
        assert_eq!(authorizer.entries, 0);
    }

    #[test]
    fn recurring_name_reuses_cached_allow_and_deny_verdicts() {
        let mut authorizer = authorizer(8);
        let token = TokenFingerprint {
            token_id: 4,
            modified_id: 5,
        };
        let generation = authorizer.generation;
        let mut checks = 0;
        assert!(
            authorizer
                .authorize_name_with(token, "allowed", || {
                    checks += 1;
                    Ok::<_, ()>((generation, true))
                })
                .unwrap()
        );
        assert!(
            authorizer
                .authorize_name_with(token, "allowed", || {
                    checks += 1;
                    Ok::<_, ()>((generation, false))
                })
                .unwrap()
        );
        assert!(
            !authorizer
                .authorize_name_with(token, "denied", || {
                    checks += 1;
                    Ok::<_, ()>((generation, false))
                })
                .unwrap()
        );
        assert!(
            !authorizer
                .authorize_name_with(token, "denied", || {
                    checks += 1;
                    Ok::<_, ()>((generation, true))
                })
                .unwrap()
        );
        assert_eq!(checks, 2);
    }
}
