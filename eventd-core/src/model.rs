//! Owned records passed from KMES drain threads to shard writers.

/// A GUID in PCDS binary layout.
pub type Guid = [u8; 16];

/// A real KMES event copied out of its producer-owned ring mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RealEvent {
    /// Kernel boot that owns the per-CPU sequence namespace.
    pub boot_id: Guid,
    /// Nanoseconds since the Unix epoch.
    pub timestamp: u64,
    /// Logical KMES CPU identifier.
    pub cpu_id: u16,
    /// Per-CPU, per-boot sequence.
    pub sequence: u64,
    /// KMES origin-class discriminant.
    pub origin_class: u8,
    /// Effective token identity stamped by KMES.
    pub effective_token_guid: Guid,
    /// True token identity stamped by KMES.
    pub true_token_guid: Guid,
    /// Process identity stamped by KMES.
    pub process_guid: Guid,
    /// Concrete event type.
    pub event_type: Box<str>,
    /// Verbatim `MessagePack` payload.
    pub payload: Box<[u8]>,
}

impl RealEvent {
    /// Exact variable-size memory charged to the handoff byte budget.
    #[must_use]
    pub fn variable_bytes(&self) -> usize {
        self.event_type.len() + self.payload.len()
    }
}

/// A missing inclusive sequence interval revealed by a real event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gap {
    /// eventd clock at gap-record generation, nanoseconds since the epoch.
    pub timestamp: u64,
    /// First missing sequence.
    pub first_sequence: u64,
    /// Last missing sequence.
    pub last_sequence: u64,
    /// Timestamp of the preceding event, when one was observed.
    pub preceding_timestamp: Option<u64>,
    /// Timestamp of the event that revealed the gap.
    pub revealing_timestamp: u64,
}

impl Gap {
    /// Number of missing sequences.
    #[must_use]
    pub const fn count(self) -> u64 {
        self.last_sequence - self.first_sequence + 1
    }
}

/// One indivisible handoff: an optional gap and the real event that revealed it.
///
/// Keeping both in one queue slot prevents a transaction boundary from splitting
/// the evidence for a gap from the revealing event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestItem {
    /// Uncovered gap intervals established before `event`.
    ///
    /// The vector is normally empty or one element. Multiple intervals occur
    /// during restart reconciliation when committed receipt islands split a
    /// missing span.
    pub gaps: Vec<Gap>,
    /// Whether the revealing event lacks a committed receipt and must be stored.
    pub store_event: bool,
    /// The real event.
    pub event: RealEvent,
}

/// A daemon-generated event written directly to an event shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticEvent {
    /// Kernel boot in which eventd generated the record.
    pub boot_id: Guid,
    /// Nanoseconds since the Unix epoch.
    pub timestamp: u64,
    /// Event type beginning with `synthetic.`.
    pub event_type: Box<str>,
    /// `MessagePack` map following the stable synthetic-event schema.
    pub payload: Box<[u8]>,
}

impl IngestItem {
    /// Exact bytes reserved before copying this item out of KMES.
    #[must_use]
    pub fn charged_bytes(&self) -> usize {
        core::mem::size_of::<Self>()
            + self.event.variable_bytes()
            + self.gaps.len() * core::mem::size_of::<Gap>()
    }
}
