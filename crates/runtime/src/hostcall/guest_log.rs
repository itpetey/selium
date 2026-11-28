//! Guest log hostcalls.
//!
//! Writes are gated by `GuestLogWrite` and may target the writer's own pid
//! or a pid it owns. Reads are gated by `GuestLogRead` and merge the legacy
//! log vector with the process's log channel.

use selium_abi::{
    AbiError, AbiErrorCode, Capability, GuestLogEntry, HostcallOutput, ProcessId, ResourceClass,
    ResourceIdentity, SharedResourceId,
};
use selium_service::{FlatMsg, log::LogRecord};

use super::HostOperationState;
use crate::{Runtime, error::kernel_error};

impl Runtime {
    pub(super) fn hostcall_guest_log_write(
        &self,
        process_id: ProcessId,
        entry: GuestLogEntry,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // GuestLogWrite treats the writer's own pid as owned: a
        // process can always write a log entry for itself.
        if entry.process_id == Some(process_id) {
            self.require(
                process_id,
                Capability::GuestLogWrite,
                ResourceClass::GuestLog,
                None,
            )?;
        } else {
            self.authorise_guest_log_process(process_id, Capability::GuestLogWrite, &entry)?;
            self.require(
                process_id,
                Capability::GuestLogWrite,
                ResourceClass::GuestLog,
                entry.process_id.map(ResourceIdentity::Local),
            )?;
        }
        self.kernel.processes().write_guest_log(entry);
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_guest_log_read(
        &self,
        process_id: ProcessId,
        cursor: usize,
        target_process_id: Option<ProcessId>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        if let Some(target_process_id) = target_process_id {
            self.ensure_local_handle_owner(
                process_id,
                Capability::GuestLogRead,
                ResourceClass::Process,
                target_process_id,
            )?;
        }
        self.require(
            process_id,
            Capability::GuestLogRead,
            ResourceClass::GuestLog,
            target_process_id.map(ResourceIdentity::Local),
        )?;

        // Read from the legacy guest_logs vec (existing path).
        let mut logs: Vec<GuestLogEntry> = self
            .kernel
            .processes()
            .read_guest_logs_from(cursor)
            .into_iter()
            .filter(|entry| target_process_id.is_none() || entry.process_id == target_process_id)
            .collect();

        // Also drain from log channels if target process has one.
        if let Some(target_pid) = target_process_id
            && let Ok(frames) = self.kernel.processes().drain_log_channel(target_pid)
        {
            for frame in frames {
                // Decode FlatBuffer LogRecord into GuestLogEntry.
                if let Ok(record) = LogRecord::decode(&frame) {
                    logs.push(GuestLogEntry {
                        process_id: Some(target_pid),
                        level: format!("{:?}", record.level),
                        target: record.target,
                        message: record.message,
                    });
                }
            }
        }

        Ok(HostOperationState::Ready(HostcallOutput::GuestLogEntries(
            logs,
        )))
    }

    pub(super) fn hostcall_guest_log_register(
        &self,
        process_id: ProcessId,
        shared_id: SharedResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Validate that shared_id belongs to the calling process.
        let owns = self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, shared_id))
            .is_some_and(|owners| owners.contains(&process_id));

        if !owns {
            return Err(AbiError::new(
                AbiErrorCode::DetachedResource,
                format!(
                    "GuestLogRegister: shared_id {shared_id} not owned by process {process_id}"
                ),
            ));
        }

        // Register the log channel with the kernel. The kernel stores
        // the shared_id per process; actual channel reading is done
        // via the kernel's shared memory primitives.
        self.kernel
            .processes()
            .register_log_channel(&self.kernel.memory(), process_id, shared_id)
            .map_err(kernel_error)?;

        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    fn authorise_guest_log_process(
        &self,
        process_id: ProcessId,
        capability: Capability,
        entry: &GuestLogEntry,
    ) -> std::result::Result<(), AbiError> {
        if let Some(entry_process_id) = entry.process_id {
            self.ensure_local_handle_owner(
                process_id,
                capability,
                ResourceClass::Process,
                entry_process_id,
            )?;
        }
        Ok(())
    }
}
