//! KMES ring discovery, restart reconciliation and drain loop.

use core::fmt;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};

use eventd_core::{
    BoundedQueue, Coverage, RealEvent, ReconcileError, Reconciler, ReserveError, StripeRouter,
};
use peios::event::{EventRing, OriginClass};

use crate::writer::WriterMessage;

pub struct Attachment {
    pub cpu_id: u16,
    pub ring: EventRing,
}

pub fn attach_all() -> Result<Vec<Attachment>, KmesError> {
    Ok(
        attach_slots(slot_count()?, peios::event::attach, |(fd, capacity)| {
            EventRing::map(fd, capacity)
        })?
        .into_iter()
        .map(|(cpu_id, ring)| Attachment { cpu_id, ring })
        .collect(),
    )
}

/// Walk every slot below `slots`, attaching through `attach` and mapping
/// through `map`, and skip each `EINVAL` hole.
fn attach_slots<D, R>(
    slots: u64,
    mut attach: impl FnMut(u32) -> Result<D, peios::Error>,
    mut map: impl FnMut(D) -> Result<R, peios::Error>,
) -> Result<Vec<(u16, R)>, KmesError> {
    let mut attachments = Vec::new();
    for logical_id in 0..slots {
        let cpu_id = u32::try_from(logical_id).map_err(|_| KmesError::TooManySlots(slots))?;
        match attach(cpu_id) {
            Ok(descriptor) => {
                let external_id =
                    u16::try_from(cpu_id).map_err(|_| KmesError::CpuIdOutOfRange(cpu_id))?;
                attachments.push((external_id, map(descriptor).map_err(KmesError::Peios)?));
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
    pub queues: Arc<[BoundedQueue<WriterMessage>]>,
    pub router: StripeRouter,
    pub coverage: Arc<Coverage>,
    pub stopping: Arc<AtomicBool>,
    pub startup: SyncSender<Result<u16, String>>,
    pub ring_pressure: Arc<[AtomicU8]>,
    pub pressure_slot: usize,
}

#[allow(
    clippy::too_many_lines,
    reason = "the ring state machine stays linear so resize and recovery ordering remain auditable"
)]
pub fn drain(attachment: Attachment, mut context: DrainContext) -> Result<Attachment, KmesError> {
    let cpu_id = attachment.cpu_id;
    let mut ring = attachment.ring;
    let mut read_position = ring.tail_pos();
    let mut generation = ring.generation();
    let mut recovery_boundary = ring.write_pos();
    let mut recovery_complete = false;
    let mut reconciler = Reconciler::new(&context.coverage, context.boot_id, cpu_id);
    let mut last_sequence = None;

    let mut final_cycle_complete = false;
    loop {
        record_pressure(
            &ring,
            read_position,
            &context.ring_pressure[context.pressure_slot],
        );
        let final_write = if context.stopping.load(Ordering::Acquire) {
            if final_cycle_complete {
                break;
            }
            final_cycle_complete = true;
            Some(ring.write_pos())
        } else {
            None
        };
        let mut progressed = false;
        loop {
            let tail = ring.tail_pos();
            if read_position < tail {
                read_position = tail;
            }
            let write = final_write.unwrap_or_else(|| ring.write_pos());
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
            let mut candidate = reconciler.clone();
            let observation = candidate
                .observe(event.sequence, event.timestamp)
                .map_err(KmesError::Sequence)?;
            // An event claiming one of the five types eventd writes itself
            // would be indistinguishable from eventd's own record, so it is
            // not stored. It is still handed to the writer, so its sequence
            // is receipted and no restart reports it lost.
            let reserved = crate::synthetic::is_reserved(event.event_type);
            if reserved && observation.store_event {
                crate::diagnostics::reserved_event_type();
            }
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
            let charged_bytes = core::mem::size_of::<WriterMessage>()
                + observation.gaps.len() * core::mem::size_of::<eventd_core::Gap>()
                + event.event_type.len()
                + event.payload.len();
            record_pressure(
                &ring,
                read_position,
                &context.ring_pressure[context.pressure_slot],
            );
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
            permit.publish(WriterMessage::Event(eventd_core::IngestItem {
                gaps: observation.gaps,
                store_event: observation.store_event && !reserved,
                event: owned,
            }));
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
            if !recovery_complete {
                recovery_boundary = ring.write_pos();
            }
            continue;
        }
        if !recovery_complete && read_position >= recovery_boundary {
            match commit_recovery_markers(&context) {
                Ok(()) => {
                    recovery_complete = true;
                    let _ = context.startup.send(Ok(cpu_id));
                }
                Err(error) => {
                    let message = error.to_string();
                    let _ = context.startup.send(Err(message));
                    return Err(error);
                }
            }
        }
        if final_cycle_complete {
            break;
        }
        if !progressed {
            ring.wait(read_position, 1_000).map_err(KmesError::Peios)?;
        }
    }

    context.ring_pressure[context.pressure_slot].store(0, Ordering::Release);
    Ok(Attachment { cpu_id, ring })
}

fn record_pressure(ring: &EventRing, read_position: u64, pressure: &AtomicU8) {
    let capacity = ring.capacity().max(1);
    let used = ring.write_pos().saturating_sub(read_position).min(capacity);
    let percent = u8::try_from(used.saturating_mul(100) / capacity).unwrap_or(100);
    pressure.store(percent, Ordering::Release);
}

fn commit_recovery_markers(context: &DrainContext) -> Result<(), KmesError> {
    let mut acknowledgements = Vec::with_capacity(context.router.shards().len());
    for &shard in context.router.shards() {
        let (sender, receiver) = sync_channel(1);
        context.queues[shard]
            .reserve(core::mem::size_of::<WriterMessage>())
            .map_err(KmesError::Queue)?
            .publish(WriterMessage::Barrier(sender));
        acknowledgements.push(receiver);
    }
    for receiver in acknowledgements {
        receiver
            .recv()
            .map_err(|_| KmesError::WriterStopped)?
            .map_err(KmesError::Writer)?;
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
    WriterStopped,
    Writer(String),
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
            Self::WriterStopped => formatter.write_str("event writer stopped during recovery"),
            Self::Writer(error) => {
                write!(formatter, "event writer failed during recovery: {error}")
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
            | Self::WriterStopped
            | Self::Writer(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A slot source that answers each slot from `answers`, recording which
    /// slots were asked about. `true` attaches; `false` is an `EINVAL` hole.
    type Attached = Result<Vec<(u16, u32)>, KmesError>;

    fn slots(answers: &[bool]) -> (Vec<u32>, Attached) {
        let mut asked = Vec::new();
        let result = attach_slots(
            u64::try_from(answers.len()).unwrap(),
            |cpu_id| {
                asked.push(cpu_id);
                if answers[usize::try_from(cpu_id).unwrap()] {
                    Ok(cpu_id)
                } else {
                    Err(peios::Error::from_raw_os_error(libc::EINVAL))
                }
            },
            Ok,
        );
        (asked, result)
    }

    #[test]
    fn an_einval_slot_is_a_hole_and_enumeration_continues_past_it() {
        let (asked, attached) = slots(&[true, false, true]);
        assert_eq!(asked, [0, 1, 2], "every slot below the count is tried");
        assert_eq!(attached.unwrap(), [(0, 0), (2, 2)]);
    }

    #[test]
    fn discovering_no_attachable_buffer_fails_startup() {
        let (asked, attached) = slots(&[false, false, false]);
        assert_eq!(asked, [0, 1, 2]);
        assert!(matches!(attached, Err(KmesError::NoBuffers)));
        let (_, attached) = slots(&[]);
        assert!(matches!(attached, Err(KmesError::NoBuffers)));
    }
}
