//! KMES ring discovery, restart reconciliation and drain loop.

use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use eventd_core::{
    BoundedQueue, Coverage, IngestItem, RealEvent, ReconcileError, Reconciler, ReserveError,
    StripeRouter,
};
use peios::event::{EventRing, OriginClass};

pub struct Attachment {
    pub cpu_id: u16,
    pub ring: EventRing,
}

pub fn attach_all() -> Result<Vec<Attachment>, KmesError> {
    let slots = slot_count()?;
    let mut attachments = Vec::new();
    for logical_id in 0..slots {
        let cpu_id = u32::try_from(logical_id).map_err(|_| KmesError::TooManySlots(slots))?;
        match peios::event::attach(cpu_id) {
            Ok((fd, capacity)) => {
                let external_id =
                    u16::try_from(cpu_id).map_err(|_| KmesError::CpuIdOutOfRange(cpu_id))?;
                attachments.push(Attachment {
                    cpu_id: external_id,
                    ring: EventRing::map(fd, capacity).map_err(KmesError::Peios)?,
                });
            }
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {}
            Err(error) => return Err(KmesError::Peios(error)),
        }
    }
    if attachments.is_empty() {
        return Err(KmesError::NoBuffers);
    }
    Ok(attachments)
}

fn slot_count() -> Result<u64, KmesError> {
    let mut slots = 0_u64;
    // SAFETY: `slots` is a writable u64 out-parameter. The call opens no fd.
    let result = unsafe { peios_sys::peios_event_slot_count(&raw mut slots) };
    if result == 0 {
        Ok(slots)
    } else {
        Err(KmesError::Peios(peios::Error::last_os_error()))
    }
}

pub struct DrainContext {
    pub boot_id: [u8; 16],
    pub queues: Arc<[BoundedQueue<IngestItem>]>,
    pub router: StripeRouter,
    pub coverage: Arc<Coverage>,
    pub stopping: Arc<AtomicBool>,
}

pub fn drain(attachment: Attachment, mut context: DrainContext) -> Result<(), KmesError> {
    let cpu_id = attachment.cpu_id;
    let mut ring = attachment.ring;
    let mut read_position = ring.tail_pos();
    let mut generation = ring.generation();
    let mut reconciler = Reconciler::new(&context.coverage, context.boot_id, cpu_id);
    let mut last_sequence = None;

    while !context.stopping.load(Ordering::Acquire) {
        let mut progressed = false;
        loop {
            let tail = ring.tail_pos();
            if read_position < tail {
                read_position = tail;
            }
            let write = ring.write_pos();
            if read_position >= write {
                break;
            }

            let (event, event_size) = ring.event_at(read_position).map_err(KmesError::Peios)?;
            if event.cpu_id != cpu_id {
                return Err(KmesError::CpuMismatch {
                    attached: cpu_id,
                    stamped: event.cpu_id,
                });
            }
            let observed_at = realtime_nanoseconds()?;
            let mut candidate = reconciler.clone();
            let observation = candidate
                .observe(event.sequence, event.timestamp, observed_at)
                .map_err(KmesError::Sequence)?;
            let needs_handoff = observation.store_event || !observation.gaps.is_empty();
            if !needs_handoff {
                if ring.tail_pos() > read_position {
                    read_position = ring.tail_pos();
                    continue;
                }
                read_position = advance(read_position, event_size)?;
                last_sequence = Some(event.sequence);
                reconciler = candidate;
                progressed = true;
                continue;
            }

            let shard = context.router.current_shard();
            let charged_bytes = core::mem::size_of::<IngestItem>()
                + observation.gaps.len() * core::mem::size_of::<eventd_core::Gap>()
                + event.event_type.len()
                + event.payload.len();
            let permit = context.queues[shard]
                .reserve(charged_bytes)
                .map_err(KmesError::Queue)?;
            let owned = RealEvent {
                boot_id: context.boot_id,
                timestamp: event.timestamp,
                cpu_id,
                sequence: event.sequence,
                origin_class: origin_discriminant(event.origin_class),
                effective_token_guid: event.effective_token_guid,
                true_token_guid: event.true_token_guid,
                process_guid: event.process_guid,
                event_type: event.event_type.into(),
                payload: event.payload.into(),
            };
            if ring.tail_pos() > read_position {
                drop(permit);
                read_position = ring.tail_pos();
                continue;
            }
            let sequence = owned.sequence;
            permit.publish(IngestItem {
                gaps: observation.gaps,
                store_event: observation.store_event,
                event: owned,
            });
            context.router.advance();
            read_position = advance(read_position, event_size)?;
            last_sequence = Some(sequence);
            reconciler = candidate;
            progressed = true;
        }

        let observed_generation = ring.generation();
        if observed_generation != generation {
            let (replacement, replacement_position) =
                replacement_ring(cpu_id, &ring, last_sequence)?;
            ring = replacement;
            read_position = replacement_position;
            generation = ring.generation();
            continue;
        }
        if !progressed {
            ring.wait(read_position, 1_000).map_err(KmesError::Peios)?;
        }
    }
    Ok(())
}

fn replacement_ring(
    cpu_id: u16,
    old_ring: &EventRing,
    last_sequence: Option<u64>,
) -> Result<(EventRing, u64), KmesError> {
    let (fd, capacity) = peios::event::attach(u32::from(cpu_id)).map_err(KmesError::Peios)?;
    let replacement = EventRing::map(fd, capacity).map_err(KmesError::Peios)?;
    let Some(last_sequence) = last_sequence else {
        let position = replacement.tail_pos();
        return Ok((replacement, position));
    };

    // `old_ring` remains borrowed and therefore mapped throughout this scan.
    // Assignment by the caller drops it only after this function succeeds.
    let _old_generation = old_ring.generation();
    let mut position = replacement.tail_pos();
    loop {
        let tail = replacement.tail_pos();
        if position < tail {
            position = tail;
        }
        if position >= replacement.write_pos() {
            return Ok((replacement, position));
        }
        let (event, size) = replacement.event_at(position).map_err(KmesError::Peios)?;
        if replacement.tail_pos() > position {
            position = replacement.tail_pos();
            continue;
        }
        if event.sequence > last_sequence {
            return Ok((replacement, position));
        }
        position = advance(position, size)?;
    }
}

const fn origin_discriminant(origin: OriginClass) -> u8 {
    match origin {
        OriginClass::Userspace => 0,
        OriginClass::Kmes => 1,
        OriginClass::Kacs => 2,
        OriginClass::Lcs => 3,
        OriginClass::Other(value) => value,
    }
}

fn advance(position: u64, event_size: usize) -> Result<u64, KmesError> {
    position
        .checked_add(u64::try_from(event_size).map_err(|_| KmesError::PositionOverflow)?)
        .ok_or(KmesError::PositionOverflow)
}

fn realtime_nanoseconds() -> Result<u64, KmesError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| KmesError::Clock)?;
    u64::try_from(elapsed.as_nanos()).map_err(|_| KmesError::Clock)
}

#[derive(Debug)]
pub enum KmesError {
    Peios(peios::Error),
    Queue(ReserveError),
    Sequence(ReconcileError),
    TooManySlots(u64),
    CpuIdOutOfRange(u32),
    CpuMismatch { attached: u16, stamped: u16 },
    NoBuffers,
    PositionOverflow,
    Clock,
}

impl fmt::Display for KmesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peios(error) => write!(formatter, "KMES API failure: {error}"),
            Self::Queue(error) => write!(formatter, "event handoff failed: {error}"),
            Self::Sequence(error) => write!(formatter, "{error}"),
            Self::TooManySlots(count) => {
                write!(formatter, "KMES slot count {count} exceeds the ABI")
            }
            Self::CpuIdOutOfRange(cpu) => {
                write!(formatter, "logical CPU ID {cpu} exceeds the event ABI")
            }
            Self::CpuMismatch { attached, stamped } => write!(
                formatter,
                "KMES ring {attached} contained an event stamped for CPU {stamped}"
            ),
            Self::NoBuffers => formatter.write_str("KMES exposed no attachable CPU buffers"),
            Self::PositionOverflow => formatter.write_str("KMES read position overflowed"),
            Self::Clock => {
                formatter.write_str("system realtime clock is outside the u64 nanosecond range")
            }
        }
    }
}

impl std::error::Error for KmesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Peios(error) => Some(error),
            Self::Queue(error) => Some(error),
            Self::Sequence(error) => Some(error),
            Self::TooManySlots(_)
            | Self::CpuIdOutOfRange(_)
            | Self::CpuMismatch { .. }
            | Self::NoBuffers
            | Self::PositionOverflow
            | Self::Clock => None,
        }
    }
}
