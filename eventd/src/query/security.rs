//! KACS-backed per-identifier and per-field query authorization.

use core::fmt;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::{HashMap, HashSet};
use std::os::fd::{AsRawFd, BorrowedFd};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use peios::registry::{CreateFlags, Key, KeyAccess, NotifyFilter, OpenFlags, ValueType};
use peios::security::SecurityDescriptor;
use peios::token::Token;

// What eventd and its clients must agree on comes from the client crate,
// so that the two cannot drift: the rights, the mapping, the namespaces
// and their root GUIDs, and the walk from an identifier to its pattern.
pub use eventd_client::access::Namespace;
use eventd_client::access::{
    EVENTD_ADMINISTER, EVENTD_PUBLISH, EVENTD_READ, GENERIC_ALL, GENERIC_EXECUTE, GENERIC_READ,
    GENERIC_WRITE, SECURITY_ROOT,
};

const EVENTD_GENERIC_MAPPING: peios_sys::kacs_generic_mapping = peios_sys::kacs_generic_mapping {
    read: GENERIC_READ,
    write: GENERIC_WRITE,
    execute: GENERIC_EXECUTE,
    all: GENERIC_ALL,
};
const DEFAULT_DESCRIPTORS: [(&str, &str, &str, Option<&str>); 5] = [
    (
        "Events",
        "*",
        "O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)",
        None,
    ),
    (
        "Logs",
        "*",
        "O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)(A;;0x00000001;;;AU)",
        None,
    ),
    (
        "Metrics",
        "*",
        "O:SYG:SYD:P(A;;0x00000009;;;SY)(A;;0x00000009;;;BA)(A;;0x00000001;;;AU)",
        Some("O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)(A;;0x00000001;;;AU)"),
    ),
    // eventd writes its own health straight into its store (TRM §5.7), so
    // nobody, not even an administrator, may publish under its prefix.
    (
        "Metrics",
        "eventd",
        "O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)(A;;0x00000001;;;AU)",
        None,
    ),
    (
        "",
        "Admin",
        "O:SYG:SYD:P(A;;0x00000004;;;SY)(A;;0x00000004;;;BA)",
        None,
    ),
];

pub fn provision_defaults() -> Result<(), SecurityError> {
    let (security, _) = Key::create(
        None,
        SECURITY_ROOT,
        KeyAccess::CREATE_SUB_KEY,
        CreateFlags::default(),
        None,
        None,
    )
    .map_err(SecurityError::Peios)?;
    for (namespace, name, sddl, legacy_sddl) in DEFAULT_DESCRIPTORS {
        let namespace_key = if namespace.is_empty() {
            None
        } else {
            let (parent, _) = Key::create(
                Some(&security),
                namespace,
                KeyAccess::CREATE_SUB_KEY,
                CreateFlags::default(),
                None,
                None,
            )
            .map_err(SecurityError::Peios)?;
            Some(parent)
        };
        let parent = namespace_key.as_ref().unwrap_or(&security);
        let (key, _) = Key::create(
            Some(parent),
            name,
            KeyAccess::QUERY_VALUE | KeyAccess::SET_VALUE,
            CreateFlags::default(),
            None,
            None,
        )
        .map_err(SecurityError::Peios)?;
        match key.query_value(b"", None) {
            Ok(value)
                if legacy_sddl.is_some_and(|legacy| {
                    value.ty == ValueType::BINARY
                        && peios::security::sddl::parse(legacy)
                            .is_ok_and(|descriptor| descriptor.as_bytes() == value.data)
                }) =>
            {
                let descriptor =
                    peios::security::sddl::parse(sddl).map_err(SecurityError::Peios)?;
                key.set_value(b"", ValueType::BINARY, descriptor.as_bytes())
                    .call()
                    .map_err(SecurityError::Peios)?;
            }
            Ok(_) => {}
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                let descriptor =
                    peios::security::sddl::parse(sddl).map_err(SecurityError::Peios)?;
                key.set_value(b"", ValueType::BINARY, descriptor.as_bytes())
                    .call()
                    .map_err(SecurityError::Peios)?;
            }
            Err(error) => return Err(SecurityError::Peios(error)),
        }
    }
    Ok(())
}

pub struct Authorizer {
    token: Token,
    descriptors: Arc<DescriptorCache>,
}

impl Authorizer {
    pub fn from_peer(
        socket: BorrowedFd<'_>,
        descriptors: Arc<DescriptorCache>,
    ) -> Result<Self, SecurityError> {
        Ok(Self {
            token: Token::open_peer(socket).map_err(SecurityError::Peios)?,
            descriptors,
        })
    }

    /// The fields of a record under `identifier` the caller may read, of
    /// `fields`, which are the record's; or `None` when the record is not
    /// visible. A record is visible when the caller may read it as a whole
    /// or any of its fields, and then holds only those (TRM §7.3).
    pub fn check(
        &self,
        namespace: Namespace,
        identifier: &str,
        fields: &[String],
    ) -> Result<Option<HashSet<String>>, SecurityError> {
        let Some((pattern, descriptor)) = self.descriptors.resolve(namespace, identifier)? else {
            return Ok(None);
        };
        let reads = eventd_client::access::field_reads(namespace.root_guid(), fields, |tree| {
            self.reads(namespace, &pattern, &descriptor, tree)
        })?;
        let granted: HashSet<String> = fields
            .iter()
            .zip(&reads.fields)
            .filter(|(_, reads)| **reads)
            .map(|(field, _)| field.clone())
            .collect();
        if !reads.record && granted.is_empty() {
            return Ok(None);
        }
        Ok(Some(granted))
    }

    /// Whether records under `identifier` may be visible to the caller with
    /// every one of `fields` readable, before any record has been read. A
    /// descriptor that grants some fields by name grants records with those
    /// fields, so those count too, though a given record may lack them.
    pub fn may_read(
        &self,
        namespace: Namespace,
        identifier: &str,
        fields: &[String],
    ) -> Result<bool, SecurityError> {
        let Some((pattern, descriptor)) = self.descriptors.resolve(namespace, identifier)? else {
            return Ok(false);
        };
        // With fields named, records are visible exactly when every one of
        // them may be read, since one readable field makes a record so.
        if !fields.is_empty() {
            let reads =
                eventd_client::access::field_reads(namespace.root_guid(), fields, |tree| {
                    self.reads(namespace, &pattern, &descriptor, tree)
                })?;
            return Ok(reads.fields.iter().all(|reads| *reads));
        }
        let tree = named_grants(namespace, &descriptor);
        let reads = self.reads(namespace, &pattern, &descriptor, &tree)?;
        Ok(reads.iter().any(|reads| *reads))
    }

    /// Whether the caller may read each node of `tree`, an object type
    /// list, under `descriptor`, the one `pattern` resolved to.
    fn reads(
        &self,
        namespace: Namespace,
        pattern: &str,
        descriptor: &SecurityDescriptor,
        tree: &[peios_sys::kacs_object_type_entry],
    ) -> Result<Vec<bool>, SecurityError> {
        if tree.len() > eventd_client::access::MAX_OBJECT_TYPES {
            return Err(SecurityError::TooManyFields);
        }
        let audit_context = audit_context(namespace_kind(namespace), Some(pattern))?;
        let request = peios_sys::peios_access_request {
            token_fd: self.token.as_raw_fd(),
            sd: descriptor.as_bytes().as_ptr().cast(),
            sd_len: descriptor.as_bytes().len(),
            desired: EVENTD_READ,
            mapping: EVENTD_GENERIC_MAPPING,
            self_sid: core::ptr::null(),
            self_sid_len: 0,
            privilege_intent: 0,
            object_tree: tree.as_ptr(),
            object_tree_count: u32::try_from(tree.len())
                .map_err(|_| SecurityError::TooManyFields)?,
            local_claims: core::ptr::null(),
            local_claims_len: 0,
            pip_type: 0,
            pip_trust: 0,
            audit_context: audit_context.as_ptr().cast(),
            audit_context_len: audit_context.len(),
        };
        let mut results = vec![
            peios_sys::kacs_node_result {
                granted: 0,
                status: 0,
            };
            tree.len()
        ];
        // SAFETY: request borrows the live token, descriptor, tree and audit
        // bytes for this call; results has exactly the advertised node count.
        let result = unsafe {
            peios_sys::peios_access_check_list(
                &raw const request,
                results.as_mut_ptr(),
                u32::try_from(results.len()).expect("tree length checked"),
            )
        };
        if result != 0 {
            return Err(SecurityError::Peios(peios::Error::last_os_error()));
        }
        Ok(results
            .iter()
            .map(|result| result.status == 0 && result.granted & EVENTD_READ != 0)
            .collect())
    }

    pub fn administer(&self) -> Result<bool, SecurityError> {
        let Some(descriptor) = self.descriptors.admin()? else {
            return Ok(false);
        };
        let audit_context = audit_context("eventd-admin", None)?;
        let request = peios_sys::peios_access_request {
            token_fd: self.token.as_raw_fd(),
            sd: descriptor.as_bytes().as_ptr().cast(),
            sd_len: descriptor.as_bytes().len(),
            desired: EVENTD_ADMINISTER,
            mapping: EVENTD_GENERIC_MAPPING,
            self_sid: core::ptr::null(),
            self_sid_len: 0,
            privilege_intent: 0,
            object_tree: core::ptr::null(),
            object_tree_count: 0,
            local_claims: core::ptr::null(),
            local_claims_len: 0,
            pip_type: 0,
            pip_trust: 0,
            audit_context: audit_context.as_ptr().cast(),
            audit_context_len: audit_context.len(),
        };
        let mut granted = 0_u32;
        // SAFETY: the request borrows the live token, descriptor and audit
        // context for this call; granted is a writable out-parameter.
        let result = unsafe {
            peios_sys::peios_access_check(
                &raw const request,
                &raw mut granted,
                core::ptr::null_mut(),
            )
        };
        if result == 0 {
            return Ok(granted & EVENTD_ADMINISTER != 0);
        }
        let error = peios::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EACCES) {
            Ok(false)
        } else {
            Err(SecurityError::Peios(error))
        }
    }

    pub fn descriptor_generation(&self) -> u64 {
        self.descriptors.generation()
    }

    /// The caller's user SID, which `MaxQueriesPerUser` counts by.
    pub fn user(&self) -> Result<peios::security::Sid, SecurityError> {
        self.token.user().map_err(SecurityError::Peios)
    }
}

/// The list `may_read` asks about when no field is named: the root, and at
/// level 1 each field `descriptor` grants by name. Their GUIDs do not say
/// their paths, so they cannot be placed beneath their prefixes; asked
/// about alone, each answers whether the caller may read records with that
/// field unless a deny on a prefix comes first, which the check of each
/// record then applies.
fn named_grants(
    namespace: Namespace,
    descriptor: &SecurityDescriptor,
) -> Vec<peios_sys::kacs_object_type_entry> {
    eventd_client::access::flat_tree(
        namespace.root_guid(),
        &eventd_client::access::field_grants(descriptor),
    )
}

/// The `object.kind` of a check against `namespace`'s patterns, as
/// eventd's fragment names it.
const fn namespace_kind(namespace: Namespace) -> &'static str {
    match namespace {
        Namespace::Events => "event-namespace",
        Namespace::Logs => "log-namespace",
        Namespace::Metrics => "metric-namespace",
    }
}

/// The audit context of a check eventd asks KACS to make, naming the
/// object it guards (PGSS §6.7): `{kind: <kind>}`, and with a pattern
/// `{kind: <kind>, <kind>: {pattern: <pattern>}}`. KACS copies it into
/// its record of the check as `object.kind` and
/// `object.<kind>.pattern`. The pattern is the one the descriptor was
/// written for, which for a log origin never holds a producer after a
/// `/`: the walk to it starts at the service.
fn audit_context(kind: &str, pattern: Option<&str>) -> Result<Vec<u8>, SecurityError> {
    let mut writer = peios::msgpack::Writer::new();
    if let Some(pattern) = pattern {
        writer
            .write_map(2)
            .write_str("kind")
            .write_str(kind)
            .write_str(kind)
            .write_map(1)
            .write_str("pattern")
            .write_str(pattern);
    } else {
        writer.write_map(1).write_str("kind").write_str(kind);
    }
    writer.to_bytes().map_err(SecurityError::Peios)
}

#[derive(Clone)]
struct ResolvedDescriptor {
    pattern: String,
    descriptor: Arc<SecurityDescriptor>,
}

struct DescriptorState {
    healthy: bool,
    resolved: HashMap<(Namespace, String), Option<ResolvedDescriptor>>,
    admin: AdminDescriptor,
}

#[derive(Clone)]
enum AdminDescriptor {
    Unresolved,
    Missing,
    Present(Arc<SecurityDescriptor>),
}

pub struct DescriptorCache {
    generation: AtomicU64,
    state: RwLock<DescriptorState>,
}

impl DescriptorCache {
    pub fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            state: RwLock::new(DescriptorState {
                healthy: true,
                resolved: HashMap::new(),
                admin: AdminDescriptor::Unresolved,
            }),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Check whether `token` may publish one concrete metric identifier.
    /// The generation returned with the verdict is the descriptor snapshot
    /// against which it was reached; callers use it to invalidate local
    /// hot-path caches without sharing those caches across threads.
    pub(crate) fn check_metric_publish(
        &self,
        token: &Token,
        identifier: &str,
    ) -> Result<(u64, bool), SecurityError> {
        loop {
            let generation = self.generation();
            let Some((pattern, descriptor)) = self.resolve(Namespace::Metrics, identifier)? else {
                if self.generation() == generation {
                    return Ok((generation, false));
                }
                continue;
            };
            // Publishing and reading share the namespace's kind: which of
            // the two was checked is the record's `access.requested`.
            let audit_context =
                audit_context(namespace_kind(Namespace::Metrics), Some(pattern.as_str()))?;
            let request = peios_sys::peios_access_request {
                token_fd: token.as_raw_fd(),
                sd: descriptor.as_bytes().as_ptr().cast(),
                sd_len: descriptor.as_bytes().len(),
                desired: EVENTD_PUBLISH,
                mapping: EVENTD_GENERIC_MAPPING,
                self_sid: core::ptr::null(),
                self_sid_len: 0,
                privilege_intent: 0,
                object_tree: core::ptr::null(),
                object_tree_count: 0,
                local_claims: core::ptr::null(),
                local_claims_len: 0,
                pip_type: 0,
                pip_trust: 0,
                audit_context: audit_context.as_ptr().cast(),
                audit_context_len: audit_context.len(),
            };
            let mut granted = 0_u32;
            // SAFETY: request borrows the live token, descriptor and audit
            // context; `granted` is a writable out-parameter for the call.
            let result = unsafe {
                peios_sys::peios_access_check(
                    &raw const request,
                    &raw mut granted,
                    core::ptr::null_mut(),
                )
            };
            let allowed = if result == 0 {
                granted & EVENTD_PUBLISH != 0
            } else {
                let error = peios::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EACCES) {
                    false
                } else {
                    return Err(SecurityError::Peios(error));
                }
            };
            if self.generation() == generation {
                return Ok((generation, allowed));
            }
        }
    }

    fn resolve(
        &self,
        namespace: Namespace,
        identifier: &str,
    ) -> Result<Option<(String, Arc<SecurityDescriptor>)>, SecurityError> {
        self.resolve_with(namespace, identifier, resolve_descriptor)
    }

    /// `resolve`, reading the registry through `load`.
    fn resolve_with(
        &self,
        namespace: Namespace,
        identifier: &str,
        load: impl Fn(Namespace, &str) -> Result<Option<(String, SecurityDescriptor)>, SecurityError>,
    ) -> Result<Option<(String, Arc<SecurityDescriptor>)>, SecurityError> {
        let key = (namespace, identifier.to_owned());
        loop {
            let (generation, healthy) = {
                let state = self
                    .state
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(cached) = state.resolved.get(&key) {
                    return Ok(cached.as_ref().map(|resolved| {
                        (resolved.pattern.clone(), Arc::clone(&resolved.descriptor))
                    }));
                }
                (self.generation(), state.healthy)
            };
            if !healthy {
                return Ok(None);
            }
            let loaded =
                load(namespace, identifier)?.map(|(pattern, descriptor)| ResolvedDescriptor {
                    pattern,
                    descriptor: Arc::new(descriptor),
                });
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.generation() != generation || !state.healthy {
                continue;
            }
            state.resolved.insert(key, loaded.clone());
            drop(state);
            return Ok(loaded.map(|resolved| (resolved.pattern, resolved.descriptor)));
        }
    }

    fn admin(&self) -> Result<Option<Arc<SecurityDescriptor>>, SecurityError> {
        self.admin_with(load_admin_descriptor)
    }

    /// `admin`, reading the registry through `load`.
    fn admin_with(
        &self,
        load: impl Fn() -> Result<Option<SecurityDescriptor>, SecurityError>,
    ) -> Result<Option<Arc<SecurityDescriptor>>, SecurityError> {
        loop {
            let (generation, healthy) = {
                let state = self
                    .state
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match &state.admin {
                    AdminDescriptor::Present(cached) => return Ok(Some(Arc::clone(cached))),
                    AdminDescriptor::Missing => return Ok(None),
                    AdminDescriptor::Unresolved => {}
                }
                (self.generation(), state.healthy)
            };
            if !healthy {
                return Ok(None);
            }
            let loaded = load()?.map(Arc::new);
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.generation() != generation || !state.healthy {
                continue;
            }
            state.admin = loaded
                .as_ref()
                .map_or(AdminDescriptor::Missing, |descriptor| {
                    AdminDescriptor::Present(Arc::clone(descriptor))
                });
            drop(state);
            return Ok(loaded);
        }
    }

    fn invalidate(&self) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.generation.fetch_add(1, Ordering::Release);
        state.healthy = true;
        state.resolved.clear();
        state.admin = AdminDescriptor::Unresolved;
    }

    fn fail_watch(&self) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.generation.fetch_add(1, Ordering::Release);
        state.healthy = false;
        state.resolved.clear();
        state.admin = AdminDescriptor::Unresolved;
    }
}

pub fn watch_descriptors(cache: &Arc<DescriptorCache>, stopping: &AtomicBool) {
    while !stopping.load(Ordering::Acquire) {
        if let Err(error) = watch_once(cache, stopping) {
            cache.fail_watch();
            eprintln!("eventd: security descriptor watch degraded: {error}");
            for _ in 0..10 {
                if stopping.load(Ordering::Acquire) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn watch_once(cache: &DescriptorCache, stopping: &AtomicBool) -> Result<(), SecurityError> {
    let key = Key::open(None, SECURITY_ROOT, KeyAccess::NOTIFY, OpenFlags::default())
        .map_err(SecurityError::Peios)?;
    key.notify(NotifyFilter::ALL, true)
        .map_err(SecurityError::Peios)?;
    key.set_nonblocking(true).map_err(SecurityError::Peios)?;
    cache.invalidate();
    let mut buffer = vec![0_u8; 64 * 1024];
    while !stopping.load(Ordering::Acquire) {
        let mut poll = libc::pollfd {
            fd: key.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll points to one initialized descriptor for the call.
        let result = unsafe { libc::poll(&raw mut poll, 1, 100) };
        if result < 0 {
            let error = peios::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(SecurityError::Peios(error));
        }
        if result == 0 {
            continue;
        }
        if poll.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            return Err(SecurityError::WatchClosed);
        }
        if !key
            .read_watch_events(&mut buffer)
            .map_err(SecurityError::Peios)?
            .is_empty()
        {
            cache.invalidate();
        }
    }
    Ok(())
}

fn load_admin_descriptor() -> Result<Option<SecurityDescriptor>, SecurityError> {
    let path = format!("{SECURITY_ROOT}\\Admin");
    let key = match Key::open(None, &path, KeyAccess::QUERY_VALUE, OpenFlags::default()) {
        Ok(key) => key,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(SecurityError::Peios(error)),
    };
    let value = match key.query_value(b"", None) {
        Ok(value) => value,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(SecurityError::Peios(error)),
    };
    if value.ty != ValueType::BINARY {
        return Err(SecurityError::InvalidDescriptorType(path));
    }
    SecurityDescriptor::from_validated_bytes(value.data)
        .map(Some)
        .map_err(SecurityError::Peios)
}

fn resolve_descriptor(
    namespace: Namespace,
    identifier: &str,
) -> Result<Option<(String, SecurityDescriptor)>, SecurityError> {
    // A log origin may name a producer within a service —
    // `jellyfin/ExecStartPre[0]`, `jobs/<guid>` — and everything from the
    // slash on is that producer, not a pattern namespace: `/` is a
    // registry path separator, so it is never written into a descriptor
    // path. Resolution therefore starts at the service, which makes a
    // service's hooks, reloads and health checks answer to the service's
    // own descriptor rather than falling through to the wildcard.
    // Event types and metric names carry no slash, so this is a log rule
    // in practice. The walk is the client crate's, which clients use to
    // tell their users what they may read.
    for pattern in eventd_client::access::candidates(identifier) {
        if let Some(descriptor) = load_descriptor(namespace, pattern)? {
            return Ok(Some((pattern.to_owned(), descriptor)));
        }
    }
    Ok(None)
}

fn load_descriptor(
    namespace: Namespace,
    pattern: &str,
) -> Result<Option<SecurityDescriptor>, SecurityError> {
    let path = eventd_client::access::descriptor_path(namespace, pattern);
    let key = match Key::open(None, &path, KeyAccess::QUERY_VALUE, OpenFlags::default()) {
        Ok(key) => key,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(SecurityError::Peios(error)),
    };
    let value = match key.query_value(b"", None) {
        Ok(value) => value,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(SecurityError::Peios(error)),
    };
    if value.ty != ValueType::BINARY {
        return Err(SecurityError::InvalidDescriptorType(path));
    }
    SecurityDescriptor::from_validated_bytes(value.data)
        .map(Some)
        .map_err(SecurityError::Peios)
}

#[derive(Debug)]
pub enum SecurityError {
    Peios(peios::Error),
    InvalidDescriptorType(String),
    TooManyFields,
    WatchClosed,
}

impl fmt::Display for SecurityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peios(error) => write!(formatter, "access-control failure: {error}"),
            Self::InvalidDescriptorType(path) => {
                write!(formatter, "security descriptor at {path} is not REG_BINARY")
            }
            Self::TooManyFields => formatter.write_str("record has too many fields to authorize"),
            Self::WatchClosed => formatter.write_str("security registry watch closed"),
        }
    }
}

impl std::error::Error for SecurityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Peios(error) => Some(error),
            Self::InvalidDescriptorType(_) | Self::TooManyFields | Self::WatchClosed => None,
        }
    }
}

#[cfg(test)]
impl Authorizer {
    /// A caller holding `token`. No socket on a test host conveys a KACS
    /// peer token, so tests that need an authorizer open the token
    /// themselves.
    pub(super) const fn for_token(token: Token, descriptors: Arc<DescriptorCache>) -> Self {
        Self { token, descriptors }
    }
}

#[cfg(test)]
impl DescriptorCache {
    /// Hold `descriptor` as `identifier`'s, found under `pattern`, as a
    /// resolution from the registry would.
    pub(super) fn resolve_as(
        &self,
        namespace: Namespace,
        identifier: &str,
        pattern: &str,
        descriptor: &SecurityDescriptor,
    ) {
        let resolved = self
            .resolve_with(namespace, identifier, |_, _| {
                Ok(Some((pattern.to_owned(), descriptor.clone())))
            })
            .expect("a resolution that reads nothing cannot fail");
        assert!(resolved.is_some());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::cell::Cell;
    use core::sync::atomic::AtomicUsize;
    use std::io::{Read as _, Write as _};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::mpsc::{Receiver, sync_channel};

    use peios::msgpack::Writer;

    use crate::commit_signal::CommitSignal;
    use crate::config::Config;
    use crate::datagram::Waker;
    use crate::indexing::{PolicyMessage, Tracker};
    use crate::metric_ingest::{RollupMaintenance, rollup_channel};
    use crate::query::{
        PerUser, QuerySocketError, QueryTuning, ServerConfig, Stores, handle, handle_as,
    };
    use peios::security::Sid;

    const READABLE: &str = "O:SYG:SYD:P(A;;0x00000001;;;AU)";

    /// A registry that answers every identifier with `READABLE`, counting
    /// the reads it is asked for.
    fn registry(
        reads: &Cell<usize>,
    ) -> impl Fn(Namespace, &str) -> Result<Option<(String, SecurityDescriptor)>, SecurityError> + '_
    {
        move |_, identifier| {
            reads.set(reads.get() + 1);
            Ok(Some((
                identifier.to_owned(),
                peios::security::sddl::parse(READABLE).unwrap(),
            )))
        }
    }

    fn admin_registry(
        reads: &Cell<usize>,
    ) -> impl Fn() -> Result<Option<SecurityDescriptor>, SecurityError> + '_ {
        move || {
            reads.set(reads.get() + 1);
            Ok(Some(peios::security::sddl::parse(READABLE).unwrap()))
        }
    }

    /// What one query connection came to when eventd could not read its
    /// peer's token: `handle`'s outcome, the bytes of the request it left
    /// unread, and every byte the client was sent.
    struct Unanswered {
        outcome: Result<(), QuerySocketError>,
        left_unread: Vec<u8>,
        answered: Vec<u8>,
        per_user: PerUser,
        streaming: Arc<AtomicUsize>,
        held: Arc<AtomicUsize>,
        policy: Receiver<PolicyMessage>,
        rollups: Receiver<RollupMaintenance>,
    }

    /// Serve `request` on one end of a socket pair. A socket pair carries no
    /// captured identity, so reading its peer token fails (on Peios with
    /// `ENODATA`; a host without KACS has no such socket option at all).
    fn serve_without_a_peer_token(request: &[u8]) -> Unanswered {
        let (mut client, server) = UnixStream::pair().unwrap();
        client.write_all(request).unwrap();
        // A second handle on eventd's end, to see afterwards what it left.
        let mut observer = server.try_clone().unwrap();
        let config = Config::test_defaults();
        let tuning = QueryTuning::from(&config);
        let runtime = config.shared();
        let (index_policy, policy) = sync_channel(1);
        let (rollup_sender, rollups) = rollup_channel(1, Waker::new().unwrap());
        let server_config = ServerConfig {
            runtime: Arc::clone(&runtime),
            index_tracker: Arc::new(Tracker::from_persisted(Vec::new(), runtime)),
            index_policy,
            rollups: rollup_sender,
            descriptors: Arc::new(DescriptorCache::new()),
        };
        let stores = Stores {
            event_paths: Vec::new(),
            log_path: PathBuf::from("/nonexistent/logs.db"),
            metric_path: PathBuf::from("/nonexistent/metrics.db"),
        };
        let streaming = Arc::new(AtomicUsize::new(0));
        let per_user = PerUser::default();
        let held = Arc::new(AtomicUsize::new(0));
        let outcome = handle(
            server,
            &stores,
            &server_config,
            &tuning,
            &streaming,
            &per_user,
            &held,
            &AtomicBool::new(false),
            &CommitSignal::new(),
            &CommitSignal::new(),
        );
        observer.set_nonblocking(true).unwrap();
        let mut left_unread = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            match observer.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => left_unread.extend_from_slice(&buffer[..read]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("reading eventd's end: {error}"),
            }
        }
        drop(observer);
        // eventd's end is closed now, so this reads everything it sent.
        let mut answered = Vec::new();
        client.read_to_end(&mut answered).unwrap();
        Unanswered {
            outcome,
            left_unread,
            answered,
            per_user,
            streaming,
            held,
            policy,
            rollups,
        }
    }

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut frame = u32::try_from(payload.len()).unwrap().to_le_bytes().to_vec();
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn uuid_v5_matches_rfc_example() {
        assert_eq!(
            eventd_core::field_guid("timestamp"),
            [
                0x67, 0x22, 0x1d, 0x34, 0xdb, 0xb9, 0x6b, 0x53, 0xb3, 0x6c, 0x94, 0xab, 0x6c, 0xd4,
                0x7e, 0x4c,
            ]
        );
    }

    // eventd-core's copy names fields in the object type lists eventd
    // builds; the client's names them in the descriptors people write.
    #[test]
    fn eventd_and_its_clients_derive_the_same_field_guids() {
        for field in [
            "event.time",
            "emitter.true-token.guid",
            "timestamp",
            "source.name",
            "granted_access",
            "core",
            "",
        ] {
            assert_eq!(
                eventd_core::field_guid(field),
                eventd_client::access::field_guid(field),
                "{field}"
            );
        }
    }

    // PEI-1288, TRM §7.3 and §B: a data type's root GUID is the object type
    // list's level-0 node only. An ACE naming it grants the record whole
    // through that node, so it adds no level-1 node of its own; listed
    // twice, AccessCheck refuses the list with EINVAL.
    #[test]
    fn an_ace_naming_the_root_guid_adds_no_level_1_node() {
        let descriptor = peios::security::sddl::parse(
            "O:SYG:SYD:P(A;;0x1;;;SY)\
             (OA;;0x1;a1b2c3d4-0001-4000-8000-000000000001;;AU)\
             (OA;;0x1;a1b2c3d4-0001-4000-8000-000000000002;;AU)\
             (OA;;0x1;e2bd1ef2-4a1f-5a4d-8b2b-6a2b43ff4a5f;;AU)",
        )
        .unwrap();
        for namespace in Namespace::ALL {
            let tree = named_grants(namespace, &descriptor);
            assert_eq!(tree[0].level, 0);
            assert_eq!(tree[0].guid, namespace.root_guid(), "the root is first");
            assert_eq!(
                tree.len(),
                2,
                "{namespace:?}: and below it only the one field granted by name"
            );
            assert_eq!(tree[1].level, 1);
            for other in Namespace::ALL {
                assert_ne!(tree[1].guid, other.root_guid(), "no root is a field");
            }
        }
    }

    // PEI-617: the list is the field paths' tree, root at level 0 and one
    // node per segment, each prefix once, in preorder.
    #[test]
    fn the_object_type_list_is_the_tree_of_the_fields_dotted_paths() {
        let fields = [
            "subject.token.sid",
            "event.time",
            "subject.process.pid",
            "subject.token.groups",
            "event.boot.guid",
            "n",
        ];
        let tree = eventd_client::access::field_tree(Namespace::Events.root_guid(), &fields);
        let expected = [
            (0, None),
            (1, Some("subject")),
            (2, Some("subject.token")),
            (3, Some("subject.token.sid")),
            (3, Some("subject.token.groups")),
            (2, Some("subject.process")),
            (3, Some("subject.process.pid")),
            (1, Some("event")),
            (2, Some("event.time")),
            (2, Some("event.boot")),
            (3, Some("event.boot.guid")),
            (1, Some("n")),
        ];
        let shape: Vec<(u16, [u8; 16])> = tree
            .entries
            .iter()
            .map(|entry| (entry.level, entry.guid))
            .collect();
        let want: Vec<(u16, [u8; 16])> = expected
            .iter()
            .map(|(level, path)| {
                (
                    *level,
                    path.map_or(Namespace::Events.root_guid(), eventd_core::field_guid),
                )
            })
            .collect();
        assert_eq!(shape, want);
        assert_eq!(
            tree.fields,
            [3, 8, 6, 4, 10, 11],
            "each field asked about is the node at the end of its path"
        );
    }

    // A prefix's GUID is the GUID of the prefix as a name, so a descriptor
    // that names `subject` names the node every subject field is beneath,
    // and one that names a full path names that field's node, as before.
    #[test]
    fn a_prefix_node_has_the_guid_of_the_prefix_and_a_field_keeps_its_own() {
        let root = Namespace::Logs.root_guid();
        let tree = eventd_client::access::field_tree(root, &["emitter.true-token.guid"]);
        let guids: Vec<[u8; 16]> = tree.entries.iter().map(|entry| entry.guid).collect();
        assert_eq!(
            guids,
            [
                root,
                eventd_core::field_guid("emitter"),
                eventd_core::field_guid("emitter.true-token"),
                eventd_core::field_guid("emitter.true-token.guid"),
            ]
        );
    }

    // Fields are not sorted into the tree as strings: `-` sorts before `.`,
    // which would put `a.b-x` between `a.b` and `a.b.d` and make KACS take
    // `a.b.d` for a child of `a.b-x`.
    #[test]
    fn a_node_is_its_paths_child_however_the_names_sort() {
        let root = Namespace::Events.root_guid();
        let tree = eventd_client::access::field_tree(root, &["a.b-x", "a.b.d", "a.b.c"]);
        let shape: Vec<(u16, [u8; 16])> = tree
            .entries
            .iter()
            .map(|entry| (entry.level, entry.guid))
            .collect();
        assert_eq!(
            shape,
            [
                (0, root),
                (1, eventd_core::field_guid("a")),
                (2, eventd_core::field_guid("a.b-x")),
                (2, eventd_core::field_guid("a.b")),
                (3, eventd_core::field_guid("a.b.d")),
                (3, eventd_core::field_guid("a.b.c")),
            ]
        );
        assert_eq!(tree.fields, [2, 4, 5]);
    }

    // A name that is a value in one place and a prefix in another, such as
    // a payload's `emitter` beside the header's `emitter.class`, is one
    // node: KACS refuses a list that names a GUID twice.
    #[test]
    fn a_field_that_is_also_a_prefix_is_one_node() {
        let root = Namespace::Events.root_guid();
        let fields = [
            "emitter.class",
            "emitter",
            "emitter.process.guid",
            "emitter",
        ];
        let tree = eventd_client::access::field_tree(root, &fields);
        assert_eq!(tree.entries.len(), 5, "root, emitter, class, process, guid");
        let mut guids: Vec<[u8; 16]> = tree.entries.iter().map(|entry| entry.guid).collect();
        guids.sort_unstable();
        guids.dedup();
        assert_eq!(guids.len(), 5, "no GUID twice");
        assert_eq!(tree.fields, [2, 1, 4, 1]);
        assert!(tree.has_children(1), "emitter has nodes beneath it");
        assert!(!tree.has_children(2) && !tree.has_children(4));
    }

    // PEI-617: a field that is also a prefix of another is checked again
    // with only its ancestors, so that what is beneath it cannot decide it;
    // every other field's verdict is its node's in the one list.
    #[test]
    fn a_field_that_is_also_a_prefix_is_checked_on_its_own() {
        let root = Namespace::Events.root_guid();
        let fields = ["emitter.class", "emitter", "n"];
        let mut lists = Vec::new();
        let reads = eventd_client::access::field_reads(root, &fields, |tree| {
            lists.push(tree.to_vec());
            // The whole tree grants everything but the root; alone,
            // `emitter` is denied.
            Ok::<_, ()>(if tree.len() == 2 {
                vec![true, false]
            } else {
                tree.iter().map(|entry| entry.level > 0).collect()
            })
        })
        .unwrap();
        assert_eq!(lists.len(), 2, "one list for all, and one for emitter");
        assert_eq!(
            lists[1]
                .iter()
                .map(|entry| (entry.level, entry.guid))
                .collect::<Vec<_>>(),
            [(0, root), (1, eventd_core::field_guid("emitter"))]
        );
        assert_eq!(reads.fields, [true, false, true]);
        assert!(!reads.record);
    }

    // KACS takes at most 1024 nodes in a list and no level limit: a deep
    // path is as many levels as it has segments.
    #[test]
    fn a_path_is_as_deep_as_its_segments_with_no_level_limit() {
        let path = (0..40)
            .map(|i| format!("s{i}"))
            .collect::<Vec<_>>()
            .join(".");
        let tree = eventd_client::access::field_tree(Namespace::Events.root_guid(), &[path]);
        assert_eq!(tree.entries.len(), 41);
        for (level, entry) in tree.entries.iter().enumerate() {
            assert_eq!(usize::from(entry.level), level);
        }
        assert_eq!(tree.fields, [40]);
    }

    #[test]
    fn default_descriptors_are_valid_and_cache_generation_advances() {
        for (_, _, sddl, _) in DEFAULT_DESCRIPTORS {
            peios::security::sddl::parse(sddl).unwrap();
        }
        let cache = DescriptorCache::new();
        assert_eq!(cache.generation(), 0);
        cache.invalidate();
        assert_eq!(cache.generation(), 1);
        cache.fail_watch();
        assert_eq!(cache.generation(), 2);
        assert!(
            !cache
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .healthy
        );
    }

    // PGSS §6.7, PEI-1279: the audit context is one map holding `kind` and,
    // under the kind's own name, the object's identifying fields, and
    // nothing else; KACS refuses any other shape.
    #[test]
    fn audit_contexts_name_the_object_by_kind_and_pattern() {
        use peios::msgpack::Reader;

        let kinds: Vec<_> = Namespace::ALL
            .iter()
            .map(|ns| namespace_kind(*ns))
            .collect();
        assert_eq!(
            kinds,
            ["event-namespace", "log-namespace", "metric-namespace"]
        );
        for kind in kinds {
            let bytes = audit_context(kind, Some("kacs.audit")).unwrap();
            let mut reader = Reader::new(&bytes);
            assert_eq!(reader.read_map().unwrap(), 2);
            assert_eq!(reader.read_str().unwrap(), "kind");
            assert_eq!(reader.read_str().unwrap(), kind);
            assert_eq!(reader.read_str().unwrap(), kind);
            assert_eq!(reader.read_map().unwrap(), 1);
            assert_eq!(reader.read_str().unwrap(), "pattern");
            assert_eq!(reader.read_str().unwrap(), "kacs.audit");
            assert_eq!(reader.remaining(), 0);
        }

        let bytes = audit_context("eventd-admin", None).unwrap();
        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.read_map().unwrap(), 1);
        assert_eq!(reader.read_str().unwrap(), "kind");
        assert_eq!(reader.read_str().unwrap(), "eventd-admin");
        assert_eq!(reader.remaining(), 0);
    }

    // The pattern a log check names is the service's: a producer after `/`
    // is never part of it (eventd.evman, object.log-namespace.pattern).
    #[test]
    fn a_log_origins_patterns_stop_before_its_producer() {
        assert_eq!(
            eventd_client::access::candidates("jellyfin.web/ExecStartPre[0]"),
            ["jellyfin.web", "jellyfin", "*"]
        );
    }

    // TRM §7.5: a failed watch discards the cache and fails closed for new
    // resolutions until the watch is re-established.
    #[test]
    fn a_failed_watch_discards_the_cache_and_fails_closed_without_reading_the_registry() {
        let cache = DescriptorCache::new();
        let reads = Cell::new(0);
        assert!(
            cache
                .resolve_with(Namespace::Events, "kacs.denied", registry(&reads))
                .unwrap()
                .is_some()
        );
        assert_eq!(reads.get(), 1, "a healthy cache reads a new identifier");

        cache.fail_watch();
        for identifier in ["service.started", "kacs.denied"] {
            assert!(
                cache
                    .resolve_with(Namespace::Events, identifier, registry(&reads))
                    .unwrap()
                    .is_none(),
                "{identifier} resolves to nothing"
            );
        }
        assert!(cache.admin_with(admin_registry(&reads)).unwrap().is_none());
        assert_eq!(
            reads.get(),
            1,
            "and the registry was not read for any of it"
        );

        // Re-establishing the watch ends the fail-closed state.
        cache.invalidate();
        assert!(
            cache
                .resolve_with(Namespace::Events, "service.started", registry(&reads))
                .unwrap()
                .is_some()
        );
        assert!(cache.admin_with(admin_registry(&reads)).unwrap().is_some());
        assert_eq!(reads.get(), 3);
    }

    // TRM §7.1, §9.3: if reading the peer token fails, the query is denied
    // entirely; there is no fallback identity.
    #[test]
    fn a_failed_peer_token_read_ends_the_connection_without_evaluating_its_query() {
        let mut request = Writer::new();
        request.write_map(1).write_str("query").write_str("EVENTS");
        let request = framed(&request.to_bytes().unwrap());
        let served = serve_without_a_peer_token(&request);

        assert!(
            matches!(served.outcome, Err(QuerySocketError::Security(_))),
            "the connection is refused for its identity: {:?}",
            served.outcome
        );
        assert!(
            served.answered.is_empty(),
            "the client is sent nothing, neither records nor a status: {:?}",
            served.answered
        );
        assert_eq!(
            served.left_unread, request,
            "the query never left the socket, so nothing evaluated it"
        );
        assert!(
            served
                .per_user
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        assert_eq!(served.streaming.load(Ordering::Acquire), 0);
        assert_eq!(served.held.load(Ordering::Acquire), 0);
        assert!(served.policy.try_recv().is_err());
        assert!(served.rollups.try_recv().is_err());
    }

    /// How a test establishes a connection's caller in place of reading its
    /// peer token.
    type Identify<'a> = &'a dyn Fn(
        BorrowedFd<'_>,
        Arc<DescriptorCache>,
    ) -> Result<(Authorizer, Sid), SecurityError>;

    // TRM §9.3: query service resumes when KACS does. A refused connection
    // leaves nothing behind, so the next one whose peer token can be read is
    // served as if nothing had failed.
    #[test]
    fn a_connection_after_a_failed_peer_token_read_is_served_once_the_read_succeeds() {
        let config = Config::test_defaults();
        let tuning = QueryTuning::from(&config);
        let runtime = config.shared();
        let (index_policy, _policy) = sync_channel(1);
        let (rollup_sender, _rollups) = rollup_channel(1, Waker::new().unwrap());
        let server_config = ServerConfig {
            runtime: Arc::clone(&runtime),
            index_tracker: Arc::new(Tracker::from_persisted(Vec::new(), runtime)),
            index_policy,
            rollups: rollup_sender,
            descriptors: Arc::new(DescriptorCache::new()),
        };
        // An empty event store: answering EVENTS needs no access check.
        let stores = Stores {
            event_paths: Vec::new(),
            log_path: PathBuf::from("/nonexistent/logs.db"),
            metric_path: PathBuf::from("/nonexistent/metrics.db"),
        };
        let streaming = Arc::new(AtomicUsize::new(0));
        let per_user = PerUser::default();
        let held = Arc::new(AtomicUsize::new(0));
        let (event_commits, log_commits) = (CommitSignal::new(), CommitSignal::new());
        let mut request = Writer::new();
        request.write_map(1).write_str("query").write_str("EVENTS");
        let request = framed(&request.to_bytes().unwrap());
        let serve = |identify: Identify<'_>| {
            let (mut client, server) = UnixStream::pair().unwrap();
            client.write_all(&request).unwrap();
            let outcome = handle_as(
                server,
                &stores,
                &server_config,
                &tuning,
                &streaming,
                &per_user,
                &held,
                &AtomicBool::new(false),
                &event_commits,
                &log_commits,
                identify,
            );
            let mut answered = Vec::new();
            // Closing a socket whose request was never read resets it.
            match client.read_to_end(&mut answered) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                Err(error) => panic!("reading the answer: {error}"),
            }
            (outcome, answered)
        };

        // KACS is gone: the peer token cannot be read.
        let (outcome, answered) = serve(&|_, _| {
            Err(SecurityError::Peios(peios::Error::from_raw_os_error(
                libc::ENOSYS,
            )))
        });
        assert!(matches!(outcome, Err(QuerySocketError::Security(_))));
        assert!(answered.is_empty());

        // KACS is back: the next connection's token is read, and it is
        // served.
        let (outcome, answered) = serve(&|_, descriptors| {
            let token = std::os::fd::OwnedFd::from(std::fs::File::open("/dev/null").unwrap());
            Ok((
                Authorizer::for_token(peios::token::Token::from(token), descriptors),
                "S-1-5-21-1-2-3-1001".parse::<Sid>().unwrap(),
            ))
        });
        assert!(outcome.is_ok(), "{outcome:?}");
        let status = |status: &str| {
            let mut frame = Writer::new();
            frame.write_map(1).write_str("status").write_str(status);
            framed(&frame.to_bytes().unwrap())
        };
        assert!(
            answered.ends_with(&status("end")),
            "an ordinary, empty result: {answered:?}"
        );
        assert!(
            !answered.windows(5).any(|window| window == b"error"),
            "and no error: {answered:?}"
        );
        assert!(
            per_user
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "neither connection still holds a slot"
        );
    }

    // TRM §7.4 step 1: the token is obtained before anything else, and
    // failing to obtain it denies the query.
    #[test]
    fn the_peer_token_is_read_before_the_request_and_a_failed_read_answers_nothing() {
        // A request eventd would refuse with an error the moment it read
        // it: its length prefix exceeds MaxQueryRequestBytes, and its body
        // is not a map.
        let mut request = u32::MAX.to_le_bytes().to_vec();
        request.extend_from_slice(b"\xa3not a map");
        let served = serve_without_a_peer_token(&request);

        assert!(
            matches!(served.outcome, Err(QuerySocketError::Security(_))),
            "the token read failed first, before any protocol check: {:?}",
            served.outcome
        );
        assert_eq!(served.left_unread, request, "the request was never read");
        assert!(
            served.answered.is_empty(),
            "and no error about it was sent: {:?}",
            served.answered
        );
    }
}
