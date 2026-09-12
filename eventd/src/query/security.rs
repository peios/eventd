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

const SECURITY_ROOT: &str = r"Machine\System\eventd\Security";
const EVENTD_READ: u32 = 0x0001;
const EVENTD_ADMINISTER: u32 = 0x0004;
const EVENTD_PUBLISH: u32 = 0x0008;
const EVENTD_GENERIC_MAPPING: peios_sys::kacs_generic_mapping = peios_sys::kacs_generic_mapping {
    read: 0x0002_0001,
    write: 0x0002_000e,
    execute: 0x0002_0001,
    all: 0x000f_000f,
};
const DEFAULT_DESCRIPTORS: [(&str, &str, &str, Option<&str>); 4] = [
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Namespace {
    Events,
    Logs,
    Metrics,
}

impl Namespace {
    const fn registry_name(self) -> &'static str {
        match self {
            Self::Events => "Events",
            Self::Logs => "Logs",
            Self::Metrics => "Metrics",
        }
    }

    const fn root_guid(self) -> [u8; 16] {
        let last = match self {
            Self::Events => 1,
            Self::Logs => 2,
            Self::Metrics => 3,
        };
        [
            0xd4, 0xc3, 0xb2, 0xa1, 0x01, 0x00, 0x00, 0x40, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, last,
        ]
    }
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

    pub fn check(
        &self,
        namespace: Namespace,
        identifier: &str,
        fields: &[String],
    ) -> Result<Option<HashSet<String>>, SecurityError> {
        let Some((pattern, descriptor)) = self.descriptors.resolve(namespace, identifier)? else {
            return Ok(None);
        };
        let mut tree = Vec::with_capacity(fields.len() + 1);
        tree.push(peios_sys::kacs_object_type_entry {
            level: 0,
            _reserved: 0,
            guid: namespace.root_guid(),
        });
        for field in fields {
            tree.push(peios_sys::kacs_object_type_entry {
                level: 1,
                _reserved: 0,
                guid: eventd_core::field_guid(field),
            });
        }
        let audit_context = format!(
            "{}:{pattern}",
            namespace.registry_name().to_ascii_lowercase()
        );
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
        if results[0].status != 0 || results[0].granted & EVENTD_READ == 0 {
            return Ok(None);
        }
        Ok(Some(
            fields
                .iter()
                .zip(&results[1..])
                .filter(|(_, result)| result.status == 0 && result.granted & EVENTD_READ != 0)
                .map(|(field, _)| field.clone())
                .collect(),
        ))
    }

    pub fn administer(&self) -> Result<bool, SecurityError> {
        let Some(descriptor) = self.descriptors.admin()? else {
            return Ok(false);
        };
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
            audit_context: b"admin".as_ptr().cast(),
            audit_context_len: 5,
        };
        let mut granted = 0_u32;
        // SAFETY: the request borrows the live token, descriptor and static
        // audit context for this call; granted is a writable out-parameter.
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
            let audit_context = format!("metric-publish:{pattern}");
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
            let loaded = resolve_descriptor(namespace, identifier)?.map(|(pattern, descriptor)| {
                ResolvedDescriptor {
                    pattern,
                    descriptor: Arc::new(descriptor),
                }
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
            let loaded = load_admin_descriptor()?.map(Arc::new);
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
    // in practice.
    let mut pattern = identifier.split('/').next().unwrap_or(identifier);
    loop {
        if let Some(descriptor) = load_descriptor(namespace, pattern)? {
            return Ok(Some((pattern.to_owned(), descriptor)));
        }
        let Some(index) = pattern.rfind('.') else {
            break;
        };
        pattern = &pattern[..index];
    }
    Ok(load_descriptor(namespace, "*")?.map(|descriptor| ("*".to_owned(), descriptor)))
}

fn load_descriptor(
    namespace: Namespace,
    pattern: &str,
) -> Result<Option<SecurityDescriptor>, SecurityError> {
    let path = format!("{SECURITY_ROOT}\\{}\\{pattern}", namespace.registry_name());
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
mod tests {
    use super::*;

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
}
