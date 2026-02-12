//! Canonical catalogue of hostcall symbols shared between host and guest.
//!
//! The entries defined here are the single source of truth for:
//! - symbol names used in `#[link(wasm_import_module = "...")]`
//! - capability → hostcall coverage (for stub generation)
//! - input/output type pairing enforced at compile time

use core::marker::PhantomData;
use std::collections::BTreeMap;

use crate::{
    Capability, GuestResourceId, ProcessStart, RkyvEncode, SessionCreate, SessionEntitlement,
    SessionRemove, SessionResource, ShmAlloc, ShmAttach, ShmDescriptor, ShmDetach, ShmRead,
    ShmShare, ShmWrite, SingletonLookup, SingletonRegister, TimeNow, TimeSleep,
};

/// Type-erased metadata describing a hostcall.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct HostcallMeta {
    /// Wasm import module name.
    pub name: &'static str,
    /// Capability required to invoke the hostcall.
    pub capability: Capability,
}

/// Typed description of a hostcall linking point.
///
/// The generic parameters ensure that the host and guest agree on ABI payloads.
pub struct Hostcall<I, O> {
    meta: HostcallMeta,
    _marker: PhantomData<(I, O)>,
}

impl<I, O> Hostcall<I, O>
where
    I: RkyvEncode + Send,
    O: RkyvEncode + Send,
    for<'a> I::Archived: 'a
        + rkyv::Deserialize<I, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> O::Archived: 'a
        + rkyv::Deserialize<O, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    /// Construct a new hostcall descriptor.
    pub const fn new(name: &'static str, capability: Capability) -> Self {
        Self {
            meta: HostcallMeta { name, capability },
            _marker: PhantomData,
        }
    }

    /// Access the symbol name.
    pub const fn name(&self) -> &'static str {
        self.meta.name
    }

    /// Access the required capability.
    pub const fn capability(&self) -> Capability {
        self.meta.capability
    }

    /// Access the type-erased metadata.
    pub const fn meta(&self) -> HostcallMeta {
        self.meta
    }
}

macro_rules! declare_hostcalls {
    (
        $( $ident:ident => {
            name: $name:literal,
            capability: $cap:path,
            input: $input:ty,
            output: $output:ty
        }, )+
    ) => {
        $(
            #[doc = concat!("Hostcall descriptor for `", $name, "`.")]
            pub const $ident: Hostcall<$input, $output> = Hostcall::new($name, $cap);
        )+

        /// Complete catalogue of hostcalls, grouped by capability.
        pub const ALL: &[HostcallMeta] = &[
            $(HostcallMeta { name: $name, capability: $cap },)+
        ];

        /// Build a map of capabilities to the hostcalls they expose.
        pub fn by_capability() -> BTreeMap<Capability, Vec<&'static HostcallMeta>> {
            let mut map = BTreeMap::new();
            for meta in ALL {
                map.entry(meta.capability)
                    .or_insert_with(Vec::new)
                    .push(meta);
            }
            map
        }

        #[doc = "Expand to the canonical hostcall symbol name for the given identifier."]
        #[macro_export]
        macro_rules! hostcall_name {
            $(($ident) => { $name };)+
            ($other:ident) => {
                compile_error!(concat!("unknown hostcall: ", stringify!($other)))
            };
        }

        #[doc = "Expand to the typed hostcall descriptor for the given identifier."]
        #[macro_export]
        macro_rules! hostcall_contract {
            $(($ident) => { &$crate::hostcalls::$ident };)+
            ($other:ident) => {
                compile_error!(concat!("unknown hostcall: ", stringify!($other)))
            };
        }
    };
}

declare_hostcalls! {
    SESSION_CREATE => {
        name: "selium::session::create",
        capability: Capability::SessionLifecycle,
        input: SessionCreate,
        output: u32
    },
    SESSION_REMOVE => {
        name: "selium::session::remove",
        capability: Capability::SessionLifecycle,
        input: SessionRemove,
        output: ()
    },
    SESSION_ADD_ENTITLEMENT => {
        name: "selium::session::add_entitlement",
        capability: Capability::SessionLifecycle,
        input: SessionEntitlement,
        output: ()
    },
    SESSION_RM_ENTITLEMENT => {
        name: "selium::session::rm_entitlement",
        capability: Capability::SessionLifecycle,
        input: SessionEntitlement,
        output: ()
    },
    SESSION_ADD_RESOURCE => {
        name: "selium::session::add_resource",
        capability: Capability::SessionLifecycle,
        input: SessionResource,
        output: u32
    },
    SESSION_RM_RESOURCE => {
        name: "selium::session::rm_resource",
        capability: Capability::SessionLifecycle,
        input: SessionResource,
        output: u32
    },
    SINGLETON_REGISTER => {
        name: "selium::singleton::register",
        capability: Capability::SingletonRegistry,
        input: SingletonRegister,
        output: ()
    },
    SINGLETON_LOOKUP => {
        name: "selium::singleton::lookup",
        capability: Capability::SingletonLookup,
        input: SingletonLookup,
        output: GuestResourceId
    },
    TIME_NOW => {
        name: "selium::time::now",
        capability: Capability::TimeRead,
        input: (),
        output: TimeNow
    },
    TIME_SLEEP => {
        name: "selium::time::sleep",
        capability: Capability::TimeRead,
        input: TimeSleep,
        output: ()
    },
    PROCESS_START => {
        name: "selium::process::start",
        capability: Capability::ProcessLifecycle,
        input: ProcessStart,
        output: GuestResourceId
    },
    PROCESS_STOP => {
        name: "selium::process::stop",
        capability: Capability::ProcessLifecycle,
        input: GuestResourceId,
        output: ()
    },
    SHM_ALLOC => {
        name: "selium::shm::alloc",
        capability: Capability::SharedMemory,
        input: ShmAlloc,
        output: ShmDescriptor
    },
    SHM_SHARE => {
        name: "selium::shm::share",
        capability: Capability::SharedMemory,
        input: ShmShare,
        output: GuestResourceId
    },
    SHM_ATTACH => {
        name: "selium::shm::attach",
        capability: Capability::SharedMemory,
        input: ShmAttach,
        output: ShmDescriptor
    },
    SHM_DETACH => {
        name: "selium::shm::detach",
        capability: Capability::SharedMemory,
        input: ShmDetach,
        output: ()
    },
    SHM_READ => {
        name: "selium::shm::read",
        capability: Capability::SharedMemory,
        input: ShmRead,
        output: Vec<u8>
    },
    SHM_WRITE => {
        name: "selium::shm::write",
        capability: Capability::SharedMemory,
        input: ShmWrite,
        output: ()
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_accessors_return_expected_metadata() {
        let meta = SESSION_CREATE.meta();
        assert_eq!(SESSION_CREATE.name(), "selium::session::create");
        assert_eq!(SESSION_CREATE.capability(), Capability::SessionLifecycle);
        assert_eq!(meta.name, SESSION_CREATE.name());
        assert_eq!(meta.capability, SESSION_CREATE.capability());
    }

    #[test]
    fn hostcall_catalogue_contains_unique_entries() {
        let mut names: Vec<&str> = ALL.iter().map(|meta| meta.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL.len());
    }

    #[test]
    fn by_capability_groups_hostcalls() {
        let grouped = by_capability();
        assert_eq!(
            grouped
                .get(&Capability::SessionLifecycle)
                .expect("session lifecycle entries")
                .len(),
            6
        );
        assert_eq!(
            grouped
                .get(&Capability::SharedMemory)
                .expect("shared memory entries")
                .len(),
            6
        );
        assert_eq!(
            grouped
                .get(&Capability::ProcessLifecycle)
                .expect("process lifecycle entries")
                .len(),
            2
        );
    }

    #[test]
    fn exported_hostcall_macros_expand_to_expected_values() {
        let name = hostcall_name!(SESSION_CREATE);
        let contract = hostcall_contract!(SESSION_CREATE);
        assert_eq!(name, "selium::session::create");
        assert_eq!(contract.name(), name);
        assert_eq!(contract.capability(), Capability::SessionLifecycle);
    }
}
