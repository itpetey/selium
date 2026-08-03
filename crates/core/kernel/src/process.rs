use std::{sync::atomic::Ordering, time::Duration};

use selium_abi::{
    ActivityEvent, ActivityKind, CapabilityGrant, GuestLogEntry, MeteringObservation,
    ProcessDescriptor, ProcessId,
};
use selium_shm::layout;

use crate::{
    Error, Result,
    state::{Kernel, ProcessState},
};

impl Kernel {
    /// Starts a process record with the supplied module, entrypoint, and grants.
    pub fn start_process(
        &self,
        module_id: impl Into<String>,
        entrypoint: impl Into<String>,
        grants: Vec<CapabilityGrant>,
    ) -> ProcessDescriptor {
        let local_id = loop {
            let counter = self.inner.next_process_id.fetch_add(1, Ordering::SeqCst);
            let id = crate::state::hashed_id(self.inner.id_seed, counter);
            if id != 0 {
                break id;
            }
        };
        let descriptor = ProcessDescriptor {
            local_id,
            module_id: module_id.into(),
            entrypoint: entrypoint.into(),
        };
        self.inner.processes.lock().insert(
            local_id,
            ProcessState {
                module_id: descriptor.module_id.clone(),
                entrypoint: descriptor.entrypoint.clone(),
                running: true,
                grants,
                log_channel_shared_id: None,
                log_channel_state: None,
            },
        );
        self.record_activity(ActivityEvent {
            kind: ActivityKind::ProcessStarted,
            process_id: Some(local_id),
            message: format!("process {} started", descriptor.module_id),
        });
        descriptor
    }

    /// Stops a running process record.
    pub fn stop_process(&self, process_id: ProcessId) -> Result<()> {
        let mut processes = self.inner.processes.lock();
        let process = processes
            .get_mut(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        if !process.running {
            return Err(Error::ProcessStopped(process_id));
        }
        process.running = false;
        self.record_activity(ActivityEvent {
            kind: ActivityKind::ProcessStopped,
            process_id: Some(process_id),
            message: format!("process {} stopped", process.module_id),
        });
        Ok(())
    }

    /// Removes a process record and associated metering.
    pub fn reap_process(&self, process_id: ProcessId) -> Result<()> {
        let removed = self.inner.processes.lock().remove(&process_id);
        if removed.is_none() {
            return Err(Error::NotFound(format!("process {process_id}")));
        }
        self.inner.metering.lock().remove(&process_id);
        Ok(())
    }

    /// Returns the descriptor for a process.
    pub fn inspect_process(&self, process_id: ProcessId) -> Result<ProcessDescriptor> {
        let processes = self.inner.processes.lock();
        let process = processes
            .get(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        Ok(ProcessDescriptor {
            local_id: process_id,
            module_id: process.module_id.clone(),
            entrypoint: process.entrypoint.clone(),
        })
    }

    /// Records an activity event and wakes activity waiters.
    pub fn record_activity(&self, event: ActivityEvent) {
        self.inner.activity_log.lock().push(event);
        self.inner.activity_log_changed.notify_all();
    }

    /// Reads activity events starting at a cursor offset.
    pub fn read_activity_from(&self, cursor: usize) -> Vec<ActivityEvent> {
        let activity_log = self.inner.activity_log.lock();
        let cursor = cursor.min(activity_log.len());
        activity_log.get(cursor..).unwrap_or_default().to_vec()
    }

    /// Waits for activity past a cursor, then returns available events.
    pub fn wait_for_activity_from(&self, cursor: usize, timeout_ms: u64) -> Vec<ActivityEvent> {
        let mut activity_log = self.inner.activity_log.lock();
        if activity_log.len() <= cursor {
            self.inner
                .activity_log_changed
                .wait_for(&mut activity_log, Duration::from_millis(timeout_ms));
        }
        let cursor = cursor.min(activity_log.len());
        activity_log.get(cursor..).unwrap_or_default().to_vec()
    }

    /// Appends a guest log entry.
    pub fn write_guest_log(&self, entry: GuestLogEntry) {
        self.inner.guest_logs.lock().push(entry);
    }

    /// Reads guest log entries starting at a cursor offset.
    pub fn read_guest_logs_from(&self, cursor: usize) -> Vec<GuestLogEntry> {
        let guest_logs = self.inner.guest_logs.lock();
        let cursor = cursor.min(guest_logs.len());
        guest_logs.get(cursor..).unwrap_or_default().to_vec()
    }

    /// Registers a shared region as the log channel for a process.
    ///
    /// The kernel attaches to the shared region as a non-blocking reader.
    /// Log entries published to the channel will be available via
    /// `read_guest_logs_from` alongside entries written via the legacy
    /// `write_guest_log` path (dual-path during transition).
    pub fn register_log_channel(
        &self,
        process_id: ProcessId,
        shared_id: selium_abi::SharedResourceId,
    ) -> Result<()> {
        let backend = self.attach_backend(shared_id)?;

        let mut processes = self.inner.processes.lock();
        let process = processes
            .get_mut(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        process.log_channel_shared_id = Some(shared_id);
        process.log_channel_state = Some(crate::state::LogChannelState {
            backend,
            read_position: 0,
        });
        Ok(())
    }

    /// Returns the log channel shared region id for a process, if registered.
    pub fn log_channel_shared_id(
        &self,
        process_id: ProcessId,
    ) -> Option<selium_abi::SharedResourceId> {
        self.inner
            .processes
            .lock()
            .get(&process_id)
            .and_then(|p| p.log_channel_shared_id)
    }

    /// Drains available frames from a process's log channel.
    ///
    /// Returns raw frame payloads (without the 12-byte header). The caller
    /// is responsible for decoding the payloads (e.g., as FlatBuffer LogRecords).
    ///
    /// Uses the shared ring frame reader with caller-managed position.
    /// Handles `Overwritten` gracefully: if the read position has been overtaken
    /// by the writer, advances to the current tail and returns available frames.
    pub fn drain_log_channel(&self, process_id: ProcessId) -> Result<Vec<Vec<u8>>> {
        let mut processes = self.inner.processes.lock();
        let process = processes
            .get_mut(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;

        let state = match process.log_channel_state.as_mut() {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let backend_ref: &dyn selium_memory::MappingBackend = &state.backend;
        let mut read_pos = state.read_position;

        // Read the ring data capacity from the shared channel header.
        let data_capacity =
            layout::load_capacity(backend_ref).map_err(|e| Error::Wasm(e.to_string()))?;
        if data_capacity == 0 || !data_capacity.is_power_of_two() {
            return Err(Error::Wasm(format!(
                "log channel has invalid capacity {data_capacity}"
            )));
        }

        let mask = data_capacity - 1;

        // Read next_tail from the shared coordination area.
        let next_tail =
            layout::load_next_tail(backend_ref).map_err(|e| Error::Wasm(e.to_string()))?;

        // If read_pos has been overtaken, skip to next_tail - capacity.
        if next_tail > read_pos + data_capacity {
            read_pos = next_tail - data_capacity;
        }

        let mut frames = Vec::new();

        while read_pos < next_tail {
            match layout::read_frame(backend_ref, read_pos, mask, data_capacity) {
                Ok(Some((header, payload))) => {
                    let frame_size = header.frame_size();
                    frames.push(payload);
                    read_pos = read_pos
                        .checked_add(frame_size)
                        .ok_or_else(|| Error::Wasm("frame size overflow".to_string()))?;
                }
                Ok(None) => break, // Frame not yet ready.
                Err(e) => return Err(Error::Wasm(e.to_string())),
            }
        }

        // Update the read position.
        state.read_position = read_pos;

        Ok(frames)
    }

    /// Returns the capability grants assigned to a process.
    pub fn process_grants(&self, process_id: ProcessId) -> Result<Vec<CapabilityGrant>> {
        let processes = self.inner.processes.lock();
        let process = processes
            .get(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        Ok(process.grants.clone())
    }

    /// Stores a metering observation for a process.
    pub fn observe_metering(&self, process_id: ProcessId, observation: MeteringObservation) {
        self.inner
            .metering
            .lock()
            .insert(process_id, observation.clone());
        self.record_activity(ActivityEvent {
            kind: ActivityKind::MeteringObserved,
            process_id: Some(process_id),
            message: format!(
                "metering updated cpu={} memory={} storage={} bandwidth={}",
                observation.cpu_micros,
                observation.memory_bytes,
                observation.storage_bytes,
                observation.bandwidth_bytes
            ),
        });
    }

    /// Returns the latest metering observation for a process, if present.
    pub fn metering_observation(&self, process_id: ProcessId) -> Option<MeteringObservation> {
        self.inner.metering.lock().get(&process_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_activity_and_metering_are_visible() {
        let kernel = Kernel::default();
        let grants = vec![CapabilityGrant::new(
            selium_abi::Capability::ProcessLifecycle,
            vec![selium_abi::ResourceSelector::Locality(
                selium_abi::LocalityScope::Cluster,
            )],
        )];
        let process = kernel.start_process("module", "main", grants.clone());
        kernel.observe_metering(
            process.local_id,
            MeteringObservation {
                cpu_micros: 10,
                memory_bytes: 20,
                storage_bytes: 30,
                bandwidth_bytes: 40,
            },
        );

        assert_eq!(
            kernel
                .inspect_process(process.local_id)
                .expect("inspect")
                .entrypoint,
            "main"
        );
        assert_eq!(
            kernel
                .metering_observation(process.local_id)
                .expect("metering")
                .cpu_micros,
            10
        );
        assert_eq!(
            kernel
                .process_grants(process.local_id)
                .expect("process grants"),
            grants
        );
        assert!(kernel.read_activity_from(0).len() >= 2);
        assert!(kernel.read_activity_from(usize::MAX).is_empty());
    }
}
