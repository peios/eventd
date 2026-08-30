//! KACS-backed per-identifier and per-field query authorization.

use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
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
const DEFAULT_DESCRIPTORS: [(&str, &str, &str); 4] = [
    (
        "Events",
        "*",
        "O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)",
    ),
    (
        "Logs",
        "*",
        "O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)(A;;0x00000001;;;AU)",
    ),
    (
        "Metrics",
        "*",
        "O:SYG:SYD:P(A;;0x00000001;;;SY)(A;;0x00000001;;;BA)(A;;0x00000001;;;AU)",
    ),
    (
        "",
        "Admin",
        "O:SYG:SYD:P(A;;0x00000004;;;SY)(A;;0x00000004;;;BA)",
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
    for (namespace, name, sddl) in DEFAULT_DESCRIPTORS {
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
            mapping: peios_sys::kacs_generic_mapping {
                read: 0x0002_0001,
                write: 0x0002_0006,
                execute: 0x0002_0001,
                all: 0x000f_0007,
            },
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
            mapping: peios_sys::kacs_generic_mapping {
                read: 0x0002_0001,
                write: 0x0002_0006,
                execute: 0x0002_0001,
                all: 0x000f_0007,
            },
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
    generation: u64,
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
    state: RwLock<DescriptorState>,
}

impl DescriptorCache {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(DescriptorState {
                generation: 0,
                healthy: true,
                resolved: HashMap::new(),
                admin: AdminDescriptor::Unresolved,
            }),
        }
    }

    fn generation(&self) -> u64 {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
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
                (state.generation, state.healthy)
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
            if state.generation != generation || !state.healthy {
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
                (state.generation, state.healthy)
            };
            if !healthy {
                return Ok(None);
            }
            let loaded = load_admin_descriptor()?.map(Arc::new);
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != generation || !state.healthy {
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
        state.generation = state.generation.wrapping_add(1);
        state.healthy = true;
        state.resolved.clear();
        state.admin = AdminDescriptor::Unresolved;
    }

    fn fail_watch(&self) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.generation = state.generation.wrapping_add(1);
        state.healthy = false;
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
    let mut pattern = identifier;
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
        for (_, _, sddl) in DEFAULT_DESCRIPTORS {
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
