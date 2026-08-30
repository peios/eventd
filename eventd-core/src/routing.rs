//! Startup-fixed shard assignment and contiguous sequence stripes.

/// Compute the shards owned by one dense CPU ordinal.
#[must_use]
pub fn assigned_shards(ordinal: usize, cpu_count: usize, shard_count: usize) -> Vec<usize> {
    assert!(cpu_count > 0 && ordinal < cpu_count);
    assert!(shard_count > 0);
    let mut shards: Vec<usize> = (ordinal..shard_count).step_by(cpu_count).collect();
    if shards.is_empty() {
        shards.push(ordinal % shard_count);
    }
    shards
}

/// Routes fixed-length contiguous event stripes across one CPU's shards.
#[derive(Debug)]
pub struct StripeRouter {
    shards: Box<[usize]>,
    stripe_length: usize,
    shard_index: usize,
    sent_in_stripe: usize,
}

impl StripeRouter {
    /// Construct a router from a non-empty shard list and positive stripe size.
    #[must_use]
    pub fn new(shards: Vec<usize>, stripe_length: usize) -> Self {
        assert!(!shards.is_empty());
        assert!(stripe_length > 0);
        Self {
            shards: shards.into_boxed_slice(),
            stripe_length,
            shard_index: 0,
            sent_in_stripe: 0,
        }
    }

    /// Select the shard for the next real event.
    pub fn next_shard(&mut self) -> usize {
        let shard = self.current_shard();
        self.advance();
        shard
    }

    /// Inspect the next shard without consuming a stripe position.
    #[must_use]
    pub fn current_shard(&self) -> usize {
        self.shards[self.shard_index]
    }

    /// Consume one successfully handed-off real event.
    pub fn advance(&mut self) {
        self.sent_in_stripe += 1;
        if self.sent_in_stripe == self.stripe_length {
            self.sent_in_stripe = 0;
            self.shard_index = (self.shard_index + 1) % self.shards.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_cpu_gets_a_path_when_shards_are_fewer() {
        assert_eq!(assigned_shards(0, 4, 2), [0]);
        assert_eq!(assigned_shards(1, 4, 2), [1]);
        assert_eq!(assigned_shards(2, 4, 2), [0]);
        assert_eq!(assigned_shards(3, 4, 2), [1]);
    }

    #[test]
    fn larger_shard_sets_follow_modulo_assignment() {
        assert_eq!(assigned_shards(0, 3, 8), [0, 3, 6]);
        assert_eq!(assigned_shards(1, 3, 8), [1, 4, 7]);
        assert_eq!(assigned_shards(2, 3, 8), [2, 5]);
    }

    #[test]
    fn routing_keeps_contiguous_stripes() {
        let mut router = StripeRouter::new(vec![1, 4, 7], 2);
        let choices: Vec<_> = (0..8).map(|_| router.next_shard()).collect();
        assert_eq!(choices, [1, 1, 4, 4, 7, 7, 1, 1]);
    }
}
