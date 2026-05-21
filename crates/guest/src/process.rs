use selium_abi::{
    ActivityEvent, CapabilityGrant, GuestLogEntry, HostcallOutput, HostcallRequest,
    MeteringObservation, ProcessDescriptor,
};

use crate::{GuestError, Result, hostcall::hostcall_ready};

#[derive(Clone, Debug)]
pub struct Process {
    descriptor: ProcessDescriptor,
}

#[derive(Clone, Copy, Debug)]
pub struct ActivityLog;

#[derive(Clone, Copy, Debug)]
pub struct Metering;

#[derive(Clone, Copy, Debug)]
pub struct GuestLog;

impl Process {
    pub fn start(
        module_id: impl Into<String>,
        entrypoint: impl Into<String>,
        arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
    ) -> Result<Self> {
        match hostcall_ready(HostcallRequest::ProcessStart {
            module_id: module_id.into(),
            entrypoint: entrypoint.into(),
            arguments,
            grants,
        })? {
            HostcallOutput::Process(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn descriptor(&self) -> &ProcessDescriptor {
        &self.descriptor
    }

    pub fn stop(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::ProcessStop {
            process_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl ActivityLog {
    pub fn read_from(cursor: usize) -> Result<Vec<ActivityEvent>> {
        match hostcall_ready(HostcallRequest::ActivityRead { cursor })? {
            HostcallOutput::ActivityEvents(events) => Ok(events),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl Metering {
    pub fn read(process_id: u64) -> Result<Option<MeteringObservation>> {
        match hostcall_ready(HostcallRequest::MeteringRead { process_id })? {
            HostcallOutput::Metering(observation) => Ok(Some(observation)),
            HostcallOutput::Empty => Ok(None),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

impl GuestLog {
    pub fn write(entry: GuestLogEntry) -> Result<()> {
        match hostcall_ready(HostcallRequest::GuestLogWrite { entry })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    pub fn read_from(cursor: usize, process_id: Option<u64>) -> Result<Vec<GuestLogEntry>> {
        match hostcall_ready(HostcallRequest::GuestLogRead { cursor, process_id })? {
            HostcallOutput::GuestLogEntries(entries) => Ok(entries),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}
