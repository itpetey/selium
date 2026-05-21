use std::{sync::atomic::Ordering, time::Duration};

use selium_abi::{
    ActivityEvent, ActivityKind, CapabilityGrant, GuestLogEntry, MeteringObservation,
    ProcessDescriptor, ProcessId,
};

use crate::{
    Error, Result,
    state::{Kernel, ProcessState},
};

impl Kernel {
    pub fn start_process(
        &self,
        module_id: impl Into<String>,
        entrypoint: impl Into<String>,
        grants: Vec<CapabilityGrant>,
    ) -> ProcessDescriptor {
        let local_id = self.inner.next_process_id.fetch_add(1, Ordering::SeqCst) + 1;
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
            },
        );
        self.record_activity(ActivityEvent {
            kind: ActivityKind::ProcessStarted,
            process_id: Some(local_id),
            message: format!("process {} started", descriptor.module_id),
        });
        descriptor
    }

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

    pub fn reap_process(&self, process_id: ProcessId) -> Result<()> {
        let removed = self.inner.processes.lock().remove(&process_id);
        if removed.is_none() {
            return Err(Error::NotFound(format!("process {process_id}")));
        }
        self.inner.metering.lock().remove(&process_id);
        Ok(())
    }

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

    pub fn record_activity(&self, event: ActivityEvent) {
        self.inner.activity_log.lock().push(event);
        self.inner.activity_log_changed.notify_all();
    }

    pub fn read_activity_from(&self, cursor: usize) -> Vec<ActivityEvent> {
        let activity_log = self.inner.activity_log.lock();
        let cursor = cursor.min(activity_log.len());
        activity_log[cursor..].to_vec()
    }

    pub fn wait_for_activity_from(&self, cursor: usize, timeout_ms: u64) -> Vec<ActivityEvent> {
        let mut activity_log = self.inner.activity_log.lock();
        if activity_log.len() <= cursor {
            self.inner
                .activity_log_changed
                .wait_for(&mut activity_log, Duration::from_millis(timeout_ms));
        }
        let cursor = cursor.min(activity_log.len());
        activity_log[cursor..].to_vec()
    }

    pub fn write_guest_log(&self, entry: GuestLogEntry) {
        self.inner.guest_logs.lock().push(entry);
    }

    pub fn read_guest_logs_from(&self, cursor: usize) -> Vec<GuestLogEntry> {
        let guest_logs = self.inner.guest_logs.lock();
        let cursor = cursor.min(guest_logs.len());
        guest_logs[cursor..].to_vec()
    }

    pub fn process_grants(&self, process_id: ProcessId) -> Result<Vec<CapabilityGrant>> {
        let processes = self.inner.processes.lock();
        let process = processes
            .get(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        Ok(process.grants.clone())
    }

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
