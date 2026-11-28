//! CPU and bandwidth metering, budget anchoring, and window rollover.

use selium_abi::{ProcessId, ResourceClass};
use wasmtiny::WasmValue;

use crate::Runtime;
use crate::runtime::CpuBudgetWindow;

impl Runtime {
    /// Projects a metering observation into the kernel.
    pub fn project_metering(
        &self,
        process_id: selium_abi::ProcessId,
        observation: selium_abi::MeteringObservation,
    ) {
        self.kernel
            .processes()
            .observe_metering(process_id, observation);
    }

    /// Accumulates bandwidth usage for a process (host instrumentation hook).
    pub fn record_bandwidth_usage(&self, process_id: selium_abi::ProcessId, bytes: u64) {
        self.metering.lock().record_bandwidth(process_id, bytes);
    }

    /// Projects a fresh metering observation for every live process into the
    /// kernel: cumulative instruction/bandwidth counters and the current
    /// memory gauge. Instruction counts and the memory gauge are read from the
    /// engine's per-instance meter (committed linear-memory pages); the
    /// bandwidth counter comes from the projector's accumulator. Tenant storage
    /// is not projected per process.
    pub fn metering_tick(&self) {
        let live: Vec<ProcessId> = self.process_authorities.lock().keys().copied().collect();

        // Refresh each live process's engine-fed readings before projecting:
        // instructions for cpu and committed linear pages for memory.
        for process_id in &live {
            self.observe_engine_metering(*process_id);
        }

        let metering = self.metering.lock();
        for process_id in live {
            let observation = metering.project(process_id);
            self.kernel
                .processes()
                .observe_metering(process_id, observation);
        }
    }

    /// Reads a live process's engine metering snapshot (executed instructions
    /// and committed linear-memory bytes) and folds it into the projector.
    /// Returns false when the process has no loaded guest to read from.
    fn observe_engine_metering(&self, process_id: ProcessId) -> bool {
        let stats = {
            let guests = self.loaded_guests.lock();
            match guests.get(&process_id).map(|guest| &guest.execution) {
                Some(crate::bootstrap::GuestExecution::Cooperative { app, module_index }) => app
                    .runtime
                    .get_module(*module_index)
                    .and_then(|module| module.stats().ok()),
                Some(crate::bootstrap::GuestExecution::Multithreaded(mt)) => mt.stats().ok(),
                None => None,
            }
        };
        let Some(stats) = stats else {
            return false;
        };
        self.metering.lock().observe_engine(
            process_id,
            stats.executed_instructions,
            stats.memory_bytes,
        );
        true
    }

    /// Re-applies each live process's tenant CPU instruction ceiling to its
    /// engine execution budget.
    ///
    /// Run on every host sampling tick so a ceiling authored or withdrawn
    /// between windows takes effect within one second; the budget *window*
    /// itself is re-anchored only when the wall-clock minute rolls (see
    /// [`Runtime::anchor_cpu_budget`]).
    pub fn refresh_cpu_budgets(&self) {
        let window_index = wall_clock_window_index();
        let live: Vec<(ProcessId, Option<String>)> = {
            let authorities = self.process_authorities.lock();
            authorities
                .iter()
                .map(|(process_id, authority)| (*process_id, authority.tenant.clone()))
                .collect()
        };
        let guests = self.loaded_guests.lock();
        for (process_id, tenant) in live {
            let Some(guest) = guests.get(&process_id) else {
                continue;
            };
            self.anchor_cpu_budget(
                process_id,
                tenant.as_deref(),
                &guest.execution,
                window_index,
            );
        }
    }

    /// Anchors a freshly loaded process's engine execution budget to its
    /// tenant's current-window CPU ceiling, before its entrypoint runs, so a
    /// process spawned mid-window is bounded from its first instructions rather
    /// than only from the next wall-clock minute.
    pub(crate) fn anchor_spawn_cpu_budget(
        &self,
        process_id: ProcessId,
        tenant: Option<&str>,
        execution: &crate::bootstrap::GuestExecution,
    ) {
        self.anchor_cpu_budget(process_id, tenant, execution, wall_clock_window_index());
    }

    /// Anchors a process's engine execution budget to its tenant's
    /// current-window CPU instruction ceiling and applies it to the process's
    /// live instance.
    ///
    /// The budget is `window_start + ceiling`, where `window_start` is the
    /// process's instruction count at the current window's start: a fresh
    /// window re-anchors from the live count (so consumption in one window
    /// never reduces the next), while a ceiling authored mid-window takes
    /// effect against the current window's remaining allowance. A tenant with
    /// no authored ceiling (or no tenant) is unbounded.
    ///
    /// The read of the live instruction count and the subsequent
    /// `set_execution_budget` are not atomic with respect to the instance's own
    /// execution, so a process already running on another thread — a poll, or a
    /// multithreaded worker — can execute a bounded step between the two; the
    /// overshoot is at most one execution step and is still metered honestly.
    fn anchor_cpu_budget(
        &self,
        process_id: ProcessId,
        tenant: Option<&str>,
        execution: &crate::bootstrap::GuestExecution,
        window_index: u64,
    ) {
        let Some(current_instructions) = instructions_of(execution) else {
            return;
        };
        let window_start_instructions = {
            let mut windows = self.cpu_budget_windows.lock();
            let window_start_instructions = anchor_window_start(
                windows.get(&process_id).copied(),
                window_index,
                current_instructions,
            );
            windows.insert(
                process_id,
                CpuBudgetWindow {
                    window_index,
                    window_start_instructions,
                },
            );
            window_start_instructions
        };
        let ceiling =
            tenant.and_then(|tenant| self.kernel.quota().lookup(tenant, ResourceClass::Cpu));
        set_execution_budget(execution, window_budget(window_start_instructions, ceiling));
    }
}

/// The start-of-window instruction count for a process: the previously anchored
/// start when the process is still in the same wall-clock window, otherwise the
/// current count (a fresh window anchors afresh).
pub(super) fn anchor_window_start(
    previous: Option<CpuBudgetWindow>,
    window_index: u64,
    current_instructions: u64,
) -> u64 {
    match previous {
        Some(window) if window.window_index == window_index => window.window_start_instructions,
        _ => current_instructions,
    }
}

/// Whether a guest reactor poll reports that the poll-owner entrypoint has
/// completed: the `__selium_guest_poll` export returns `1` when the entrypoint
/// future finished (a still-running, parked entrypoint returns `0`).
pub(super) fn entrypoint_completed(results: &[WasmValue]) -> bool {
    results.first() == Some(&WasmValue::I32(1))
}

/// Reads a guest execution's cumulative executed-instruction count, or `None`
/// when its instance is unavailable.
fn instructions_of(execution: &crate::bootstrap::GuestExecution) -> Option<u64> {
    match execution {
        crate::bootstrap::GuestExecution::Cooperative { app, module_index } => app
            .runtime
            .get_module(*module_index)
            .and_then(|module| module.stats().ok())
            .map(|stats| stats.executed_instructions),
        crate::bootstrap::GuestExecution::Multithreaded(mt) => {
            mt.stats().ok().map(|stats| stats.executed_instructions)
        }
    }
}

/// Applies an engine execution budget to a guest execution (`None` clears any
/// budget). Both engine paths carry an instruction meter (the interpreter
/// charges every executed instruction; the AOT path charges size-weighted fuel
/// at entries and loop back-edges), so the budget applies to a cooperative
/// process's instance or a multithreaded process's shared worker-pool instance
/// alike.
fn set_execution_budget(execution: &crate::bootstrap::GuestExecution, budget: Option<u64>) {
    match execution {
        crate::bootstrap::GuestExecution::Cooperative { app, module_index } => {
            if let Some(module) = app.runtime.get_module(*module_index) {
                drop(module.set_execution_budget(budget));
            }
        }
        crate::bootstrap::GuestExecution::Multithreaded(mt) => {
            drop(mt.set_execution_budget(budget));
        }
    }
}

/// The wall-clock minute index (`unix_seconds / 60`) that process CPU budget
/// windows are aligned to — the same UTC-minute boundaries the accountant's
/// per-minute billing windows use, so a process's budget window matches its
/// billing window. A pre-epoch system clock falls back to a process-monotonic
/// minute so windows still roll rather than pinning at zero.
pub(super) fn wall_clock_window_index() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_secs() / 60,
        Err(_) => {
            static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
            EPOCH
                .get_or_init(std::time::Instant::now)
                .elapsed()
                .as_secs()
                / 60
        }
    }
}

/// A process's engine execution budget for a window: the window's anchored
/// start count plus the tenant's ceiling (`None` = unbounded).
pub(super) fn window_budget(window_start_instructions: u64, ceiling: Option<u64>) -> Option<u64> {
    ceiling.map(|ceiling| window_start_instructions.saturating_add(ceiling))
}
